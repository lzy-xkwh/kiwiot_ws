import logging
import aiohttp
import asyncio
import re
import random
import json
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any
from ..entity.lock import KiwiLockEvent, KiwiLockCamera, KiwiLockStatus
from ..lock import KiwiLockEntity
from ..battery_sensor import KiwiLockBatterySensor
from ..const import LOGGER_NAME, WS_URL, DOMAIN
from .utils import convert_wsevent_format, convert_media_event_format, get_latest_event
from homeassistant.helpers.dispatcher import async_dispatcher_send
from .userinfo import get_llock_userinfo, get_llock_info
from .token_manager import TokenManager
from ..event_utils import event_name, event_label, lock_state

_LOGGER = logging.getLogger(f"{LOGGER_NAME}_{__name__}")

response_futures: Dict[str, asyncio.Future] = {}

# WebSocket 健康检查参数
WS_SYNC_INTERVAL = 60        # 兜底同步循环的检查间隔（秒）
WS_QUIET_WINDOW = 180        # WebSocket 静默超过该时长即视为异常，加快兜底频率
WS_RECONCILE_INTERVAL = 300  # 连接健康时的周期对账间隔（秒）
WS_STALE_TIMEOUT = 90        # 超过该时长没收到任何入站消息（含 Pong）即断开重连
WS_MIN_STABLE_SECONDS = 60   # 连接稳定存活该时长后重置重连退避

async def generate_uuid() -> str:
    """生成符合特定格式的 UUID 字符串。"""
    def replace_x_or_y(match):
        char = match.group(0)
        return hex(random.randint(8, 11))[2] if char == 'y' else hex(random.randint(0, 15))[2]

    uuid_pattern = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'
    return re.sub(r'[xy]', replace_x_or_y, uuid_pattern)

async def start_websocket_connection(hass, entry, session):
    """启动 WebSocket 连接并维护心跳和消息处理."""
    token_manager = TokenManager(hass, entry)
    base_retry_delay = 5

    msg_queue = asyncio.Queue()
    domain_data = hass.data.get(DOMAIN)
    if domain_data is None:
        _LOGGER.warning("集成数据不存在，不启动 WebSocket 连接")
        return
    domain_data["msg_queue"] = msg_queue
    domain_data.setdefault("last_ws_message", 0.0)
    domain_data.setdefault("last_event_at", {})

    retry_count = 0
    resync_task = asyncio.create_task(resync_loop(hass, entry, session))
    try:
        while True:
            if session.closed:
                _LOGGER.warning("Session已关闭,停止WebSocket连接")
                return
            if DOMAIN not in hass.data:
                _LOGGER.warning("集成已被移除,停止WebSocket连接")
                return

            connected_at = None
            try:
                access_token = await token_manager.get_token(session)
                if not access_token:
                    raise RuntimeError("无法获取有效的 access_token")
                ws_url = f"{WS_URL}/?access_token={access_token}"

                async with session.ws_connect(ws_url) as ws:
                    domain_data["ws"] = ws
                    connected_at = time.monotonic()
                    domain_data["last_ws_message"] = connected_at
                    if retry_count:
                        # 重连成功用 WARNING 记一条，默认日志级别下也能看到断线恢复
                        _LOGGER.warning("WebSocket 重连成功 (已重试 %s 次)", retry_count)
                    else:
                        _LOGGER.info("WebSocket 连接已建立")

                    tasks = [
                        asyncio.create_task(send_heartbeat(ws, domain_data), name="kiwiot-heartbeat"),
                        asyncio.create_task(handle_websocket_messages(ws, hass, entry), name="kiwiot-receiver"),
                        asyncio.create_task(process_message_queue(ws, msg_queue), name="kiwiot-sender")
                    ]

                    # 用 FIRST_COMPLETED 而不是 FIRST_EXCEPTION：接收循环在服务端
                    # 正常关闭连接（收到 CLOSED 帧）时会“无异常地返回”，
                    # FIRST_EXCEPTION 此时不会唤醒，配合永不结束的消息队列任务，
                    # 监控协程会永久卡住 —— 连接静默死亡且永不重连（事件全部丢失）。
                    try:
                        done, _pending = await asyncio.wait(
                            tasks,
                            return_when=asyncio.FIRST_COMPLETED
                        )
                    finally:
                        for task in tasks:
                            if not task.done():
                                task.cancel()
                        await asyncio.gather(*tasks, return_exceptions=True)
                        domain_data["ws"] = None

                    for task in done:
                        if task.cancelled():
                            detail = "被取消"
                        else:
                            exc = task.exception()
                            detail = f"异常 {exc!r}" if exc else "正常结束（无异常）"
                        # close_code 是判断“为什么断”的关键证据：
                        # 1000=正常关闭, 1006=异常断开(无关闭帧), 4xxx=服务端业务原因
                        _LOGGER.warning(
                            "WebSocket 子任务结束，连接已不可用，准备重连: %s | %s | close_code=%s",
                            task.get_name(), detail, ws.close_code,
                        )

            except asyncio.CancelledError:
                _LOGGER.info("WebSocket任务被取消")
                raise
            except aiohttp.ClientError as e:
                if "Session is closed" in str(e):
                    _LOGGER.warning("Session已关闭,停止重试")
                    return
                _LOGGER.error(f"WebSocket 连接错误: {e}")
                retry_count += 1
            except Exception as e:
                _LOGGER.error(f"WebSocket 连接发生未知异常: {e}")
                retry_count += 1

            # 连接稳定存活过一段时间，视为链路已恢复，重置退避
            if connected_at and (time.monotonic() - connected_at) >= WS_MIN_STABLE_SECONDS:
                retry_count = 0

            # 无限重连，除非集成被移除或 session 关闭
            if DOMAIN not in hass.data or session.closed:
                _LOGGER.warning("集成已被移除或session已关闭,停止重连")
                return
            wait_time = base_retry_delay * min(10, 2 ** retry_count)
            _LOGGER.info(f"等待 {wait_time} 秒后重试连接 (当前重试次数: {retry_count})")
            await asyncio.sleep(wait_time)
    finally:
        resync_task.cancel()

async def send_heartbeat(ws, domain_data=None):
    """发送心跳消息，并在长时间收不到任何入站消息时主动断开以触发重连。

    服务端会对 Iot.Application/Ping 回 Pong（实测每 30 秒一次），因此
    “超过 90 秒没有任何入站消息”就是连接已死（含 TCP 半开）的可靠信号。
    """

    interval = 30
    try:
        while not ws.closed:
            if domain_data is not None:
                last_message = domain_data.get("last_ws_message") or 0.0
                if (time.monotonic() - last_message) > WS_STALE_TIMEOUT:
                    _LOGGER.warning(
                        "超过 %s 秒未收到任何服务端消息（含 Pong），判定连接失效并重连",
                        WS_STALE_TIMEOUT,
                    )
                    await ws.close()
                    return
            uuid = await generate_uuid()
            ping_message = {
                "header": {
                    "namespace": "Iot.Application",
                    "name": "Ping",
                    "messageId": uuid,
                    "payloadVersion": 1
                }
            }
            await ws.send_json(ping_message)
            _LOGGER.debug("心跳消息已发送")
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        _LOGGER.info("心跳任务被取消")
        raise
    except Exception as e:
        _LOGGER.error(f"心跳消息发送失败: {e}")
        raise

async def send_unlock_command(hass, send_token, unlock_data, device_id) -> bool:
    """发送开锁指令"""
    try:
        msg_queue = hass.data.get(DOMAIN, {}).get("msg_queue")
        if not msg_queue:
            _LOGGER.error("消息队列未初始化")
            return False
            
        uuid = await generate_uuid()
        msg = {
            "header": {
                "namespace": "Iot.Device",
                "name": "Ctrl", 
                "messageId": uuid,
                "payloadVersion": 1,
                "secureToken": send_token
            },
            "payload": {
                "did": device_id,
                "verify": True,
                "data": unlock_data
            }
        }
        
        await msg_queue.put(msg)
        _LOGGER.debug(f"开锁指令已加入队列: {device_id}")
        return True
        
    except Exception as e:
        _LOGGER.error(f"发送开锁指令失败: {e}")
        return False

async def handle_websocket_messages(ws, hass, entry):
    """处理 WebSocket 消息并更新实体状态"""
    try:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    domain_state = hass.data.get(DOMAIN)
                    if domain_state is not None:
                        domain_state["last_ws_message"] = time.monotonic()
                    _LOGGER.debug("接收到消息: %s", data)
                    
                    header = data.get("header", {})
                    if header.get("name") == "CtrlResponse":
                        message_id = header.get("messageId")
                        if message_id in response_futures:
                            response_futures[message_id].set_result(data)
                            del response_futures[message_id]
                            continue
                    
                    if (header.get("namespace") == "Iot.Device" and 
                        header.get("name") == "EventNotify"):
                        await process_device_event(hass, entry, data)
                        
                except json.JSONDecodeError as e:
                    _LOGGER.error(f"JSON解析错误: {e}")
                    continue
                    
            elif msg.type == aiohttp.WSMsgType.CLOSE:
                # 服务端主动关闭：msg.data 是关闭码，msg.extra 是原因文本
                _LOGGER.warning(
                    "服务端请求关闭连接: code=%s reason=%r", msg.data, msg.extra
                )
            elif msg.type == aiohttp.WSMsgType.CLOSING:
                _LOGGER.warning("WebSocket 正在关闭: close_code=%s", ws.close_code)
            elif msg.type == aiohttp.WSMsgType.ERROR:
                _LOGGER.error("WebSocket 错误: %r", msg.data)
                break
            elif msg.type == aiohttp.WSMsgType.CLOSED:
                _LOGGER.warning("WebSocket 连接已关闭: close_code=%s", ws.close_code)
                break
            
    except asyncio.CancelledError:
        _LOGGER.info("WebSocket消息处理任务被取消")
        raise
    except Exception as e:
        _LOGGER.error(f"处理 WebSocket 消息时发生错误: {e}")
        for future in response_futures.values():
            if not future.done():
                future.set_exception(e)
        raise
    finally:
        _LOGGER.info("接收循环结束: close_code=%s", ws.close_code)
        for future in response_futures.values():
            if not future.done():
                future.cancel()

async def process_device_event(hass, entry, data):
    """处理设备事件通知"""
    payload = data.get("payload", {})
    device_id = payload.get("did")
    event_type = payload.get("level")
    normalized_name = event_name(payload)
    
    try:
        await update_battery_state(hass, device_id, payload)
        if normalized_name in {"UNLOCKED", "LOCKED", "LOCK_INDOOR_BUTTON_UNLOCK", "LOCK_INDOOR_LEVER_UNLOCK"}:
            payload = await convert_wsevent_format(payload)
        elif event_type == "CRITICAL" and normalized_name == "REMOTE_UNLOCK":
            payload = await convert_media_event_format(payload)
        else:
            # Keep the original payload for alarms, user changes, battery and
            # future server-side event types instead of dropping them.
            payload = dict(payload)
            payload["name"] = normalized_name or "UNKNOWN"
            payload.setdefault("data", {})
            _LOGGER.debug("Received KiwiOT event: %s (%s)", event_label(payload), event_type)
        await apply_device_event(hass, entry, device_id, payload)

    except Exception as e:
        _LOGGER.error("处理设备事件失败: %s", e)


async def apply_device_event(hass, entry, device_id, payload):
    """把事件写入实体并广播 kiwiot_ws_event 总线事件（WebSocket 与兜底同步共用）。"""
    await update_device_state(hass, entry, device_id, payload)
    hass.bus.async_fire(
        f"{DOMAIN}_event",
        {
            "device_id": device_id,
            "event": event_name(payload) or "UNKNOWN",
            "label": event_label(payload),
            "level": payload.get("level"),
            "created_at": payload.get("created_at"),
            "data": payload.get("data", {}),
        },
    )
    created_at = _event_created_at(payload)
    domain_state = hass.data.get(DOMAIN)
    if created_at and domain_state is not None:
        domain_state.setdefault("last_event_at", {})[device_id] = created_at

async def update_battery_state(hass, device_id, payload):
    """Update a lock battery sensor directly from an unmodified payload."""
    device_entities = (
        hass.data.get(DOMAIN, {}).get("devices", {}).get(device_id, [])
    )
    for entity in device_entities:
        if isinstance(entity, KiwiLockBatterySensor):
            if entity.update_from_payload(payload):
                entity.async_write_ha_state()


async def update_device_state(hass, entry, device_id, event_data):
    """根据WebSocket消息更新设备实体状态"""
    try:
        domain_data = hass.data.get(DOMAIN, {})
        device_entities = domain_data.get("devices", {}).get(device_id, [])
        
        if not device_entities:
            _LOGGER.warning(f"未找到设备ID {device_id} 对应的实体")
            return
            
        session = domain_data.get("session")
        if not session:
            _LOGGER.error("无法获取会话实例")
            return
            
        users = await get_llock_userinfo(hass, entry, device_id, session)
        
        update_tasks = []
        for entity in device_entities:
            if isinstance(entity, KiwiLockBatterySensor):
                if entity.update_from_payload(event_data):
                    entity.async_write_ha_state()
            elif isinstance(entity, KiwiLockEntity):
                if lock_state(event_data) is not None:
                    entity.update_from_event(event_data)
                    entity.async_write_ha_state()
            elif isinstance(entity, KiwiLockEvent):
                update_tasks.append(update_lock_event(entity, event_data, users))
            elif isinstance(entity, KiwiLockStatus) and lock_state(event_data) is not None:
                update_tasks.append(update_lock_status(entity, event_data))
            elif isinstance(entity, KiwiLockCamera) and event_data.get("data"):
                update_tasks.append(update_camera(entity, event_data))
                
        await asyncio.gather(*update_tasks, return_exceptions=True)
        async_dispatcher_send(hass, f"{DOMAIN}_{device_id}_update")
            
    except Exception as e:
        _LOGGER.error(f"更新设备状态失败: {e}，错误数据结构：{event_data}")

async def update_lock_event(entity, event_data, users):
    """更新门锁事件实体"""
    try:
        entity._event = event_data
        entity._users = users
        if "created_at" in event_data:
            timestamp = datetime.fromisoformat(
                event_data["created_at"].replace('Z', '+00:00')
            ).astimezone(ZoneInfo("Asia/Shanghai"))
            entity._event_time = timestamp.strftime("%Y-%m-%d %H:%M:%S")
            entity._notify_time = timestamp.strftime("%H:%M:%S")
        await entity.async_update_ha_state(True)
        _LOGGER.debug(f"已更新设备事件状态: {entity}")
    except Exception as e:
        _LOGGER.error(f"更新门锁事件失败: {e}")

async def update_lock_status(entity, event_data):
    """更新门锁状态实体"""
    try:
        entity._event = event_data
        if "created_at" in event_data:
            timestamp = datetime.fromisoformat(
                event_data["created_at"].replace('Z', '+00:00')
            ).astimezone(ZoneInfo("Asia/Shanghai"))
            entity._event_time = timestamp.strftime("%Y-%m-%d %H:%M:%S")
            entity._notify_time = timestamp.strftime("%H:%M:%S")
        await entity.async_update_ha_state(True)
        _LOGGER.debug(f"已更新门锁状态: {entity}")
    except Exception as e:
        _LOGGER.error(f"更新门锁状态失败: {e}")

async def update_camera(entity, event_data):
    """更新相机实体"""
    try:
        await entity.update_from_event(event_data)
        await entity.async_update_ha_state(True)
        _LOGGER.debug("已更新相机状态和图片")
    except Exception as e:
        _LOGGER.error(f"更新相机状态失败: {e}")

async def stop_websocket_connection(websocket_task):
    """停止 WebSocket 连接任务。"""
    if not websocket_task:
        return
        
    try:
        websocket_task.cancel()
        await websocket_task
        _LOGGER.info("WebSocket 连接任务已成功停止")
    except asyncio.CancelledError:
        _LOGGER.info("WebSocket 连接任务被取消")
    except Exception as e:
        _LOGGER.error(f"停止 WebSocket 连接任务时发生错误: {e}")

async def process_message_queue(ws, queue):
    """处理消息队列中的请求"""
    try:
        while True:
            try:
                msg = await queue.get()
                if ws.closed:
                    _LOGGER.error("WebSocket已关闭,无法发送消息")
                    queue.task_done()
                    continue

                await ws.send_json(msg)
                _LOGGER.debug(f"消息队列发送消息: {msg}")
                queue.task_done()
                    
            except asyncio.CancelledError:
                _LOGGER.info("消息队列处理被取消")
                raise
            except Exception as e:
                _LOGGER.error(f"处理消息队列异常: {e}")
                if not queue.empty():
                    queue.task_done()
                
    except asyncio.CancelledError:
        _LOGGER.info("消息队列处理任务结束")
        raise
    finally:
        for fut in response_futures.values():
            if not fut.done():
                fut.cancel()


async def resync_loop(hass, entry, session):
    """REST 兜底同步：WebSocket 长时间没有任何消息时主动拉取最新事件。

    WebSocket 可能“连接还在、但服务端不再推送”（半开连接、服务端静默丢弃订阅等）。
    这类故障不产生任何异常，只靠重连逻辑发现不了。这里用 REST 轮询兜底，
    保证即使 WebSocket 彻底哑掉，事件最多延迟一个轮询周期也能补进实体。
    """
    last_reconcile = 0.0
    while True:
        try:
            await asyncio.sleep(WS_SYNC_INTERVAL)
            if DOMAIN not in hass.data or session.closed:
                return
            now = time.monotonic()
            last_message = hass.data[DOMAIN].get("last_ws_message") or 0.0
            quiet = (now - last_message) >= WS_QUIET_WINDOW
            if not quiet and (now - last_reconcile) < WS_RECONCILE_INTERVAL:
                # 连接在正常收 Pong/事件，按低频节奏对账即可（约每 5 分钟一次）
                continue
            last_reconcile = now
            await sync_latest_events(hass, entry, session)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _LOGGER.debug("REST 兜底同步失败: %s", e)


async def sync_latest_events(hass, entry, session):
    """拉取每台设备的最新事件，比实体当前事件更新则补写。"""
    devices = hass.data.get(DOMAIN, {}).get("devices", {})
    for device_id in list(devices):
        try:
            events = await get_llock_info(hass, entry, device_id, session)
            latest = await get_latest_event(events) if events else None
            if not latest or not _is_event_newer(hass, device_id, latest):
                continue
            payload = _normalize_rest_event(latest, device_id)
            _LOGGER.warning(
                "WebSocket 静默，已通过 REST 补回事件: %s %s (%s)",
                device_id, payload.get("name"), payload.get("created_at"),
            )
            await apply_device_event(hass, entry, device_id, payload)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _LOGGER.debug("同步设备 %s 事件失败: %s", device_id, e)


def _event_created_at(event):
    if isinstance(event, dict) and event.get("created_at"):
        return str(event["created_at"])
    return None


def _parse_event_time(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _entity_event_created_at(hass, device_id):
    entities = hass.data.get(DOMAIN, {}).get("devices", {}).get(device_id, [])
    for entity in entities:
        for attr in ("_event", "_event_data"):
            created_at = _event_created_at(getattr(entity, attr, None))
            if created_at:
                return created_at
    return None


def _is_event_newer(hass, device_id, event):
    """判断事件是否比实体当前事件更新（首次同步以实体当前事件为基线）。"""
    created_at = _event_created_at(event)
    domain_state = hass.data.get(DOMAIN)
    if not created_at or domain_state is None:
        return False
    markers = domain_state.setdefault("last_event_at", {})
    previous = markers.get(device_id) or _entity_event_created_at(hass, device_id)
    if not previous:
        markers[device_id] = created_at
        return False
    new_time = _parse_event_time(created_at)
    old_time = _parse_event_time(previous)
    if new_time is None or old_time is None:
        if created_at == previous:
            return False
        markers[device_id] = created_at
        return True
    if new_time <= old_time:
        return False
    markers[device_id] = created_at
    return True


def _normalize_rest_event(event, device_id):
    """把 REST 事件整理成和 WebSocket 事件一致的结构。"""
    payload = dict(event)
    payload.setdefault("did", device_id)
    payload["name"] = event_name(payload) or "UNKNOWN"
    data = dict(payload.get("data") or {})
    if "image" not in data and data.get("image_uri"):
        data["image"] = {"uri": data["image_uri"]}
    payload["data"] = data
    return payload
