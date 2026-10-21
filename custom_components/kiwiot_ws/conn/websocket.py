import logging
import aiohttp
import asyncio
import re
import random
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any
from ..entity.lock import KiwiLockEvent, KiwiLockCamera, KiwiLockStatus
from ..lock import KiwiLockEntity
from ..battery_sensor import KiwiLockBatterySensor
from ..const import LOGGER_NAME, WS_URL, DOMAIN
from .utils import convert_wsevent_format, convert_media_event_format
from homeassistant.helpers.dispatcher import async_dispatcher_send
from .userinfo import get_llock_userinfo
from .token_manager import TokenManager
from ..event_utils import event_name, event_label, lock_state

_LOGGER = logging.getLogger(f"{LOGGER_NAME}_{__name__}")

response_futures: Dict[str, asyncio.Future] = {}

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
    hass.data[DOMAIN]["msg_queue"] = msg_queue

    retry_count = 0
    while True:
        try:
            if session.closed:
                _LOGGER.warning("Session已关闭,停止WebSocket连接") 
                return
            if DOMAIN not in hass.data:
                _LOGGER.warning("集成已被移除,停止WebSocket连接")
                return

            access_token = await token_manager.get_token(session)
            ws_url = f"{WS_URL}/?access_token={access_token}"

            async with session.ws_connect(ws_url) as ws:
                hass.data[DOMAIN]["ws"] = ws
                _LOGGER.info(f"WebSocket 连接已建立 (重试次数: {retry_count})")

                tasks = [
                    asyncio.create_task(send_heartbeat(ws)),
                    asyncio.create_task(handle_websocket_messages(ws, hass, entry)),
                    asyncio.create_task(process_message_queue(ws, msg_queue))
                ]

                try:
                    done, pending = await asyncio.wait(
                        tasks, 
                        return_when=asyncio.FIRST_EXCEPTION
                    )

                    for task in pending:
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass

                    for task in done:
                        exc = task.exception()
                        if exc:
                            _LOGGER.error(f"WebSocket任务异常: {exc}")
                            raise exc

                except asyncio.CancelledError:
                    _LOGGER.info("WebSocket任务被取消")
                    return

        except aiohttp.ClientError as e:
            if "Session is closed" in str(e):
                _LOGGER.warning("Session已关闭,停止重试")
                return
            _LOGGER.error(f"WebSocket 连接错误: {e}")
            retry_count += 1

        except Exception as e:
            _LOGGER.error(f"WebSocket 连接发生未知异常: {e}")
            retry_count += 1

        # 无限重连，除非集成被移除或 session 关闭
        if DOMAIN not in hass.data or session.closed:
            _LOGGER.warning("集成已被移除或session已关闭,停止重连")
            return
        wait_time = base_retry_delay * min(10, 2 ** retry_count)
        _LOGGER.info(f"等待 {wait_time} 秒后重试连接 (当前重试次数: {retry_count})")
        await asyncio.sleep(wait_time)

async def send_heartbeat(ws):
    """发送心跳消息。"""

    interval = 30
    try:
        while not ws.closed:
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
                    
            elif msg.type == aiohttp.WSMsgType.ERROR:
                _LOGGER.error(f"WebSocket 错误: {msg.data}")
                break
            elif msg.type == aiohttp.WSMsgType.CLOSED:
                _LOGGER.warning("WebSocket 连接已关闭")
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
        await update_device_state(hass, entry, device_id, payload)
        hass.bus.async_fire(
            f"{DOMAIN}_event",
            {
                "device_id": device_id,
                "event": normalized_name or "UNKNOWN",
                "label": event_label(payload),
                "level": event_type,
                "created_at": payload.get("created_at"),
                "data": payload.get("data", {}),
            },
        )
        
    except Exception as e:
        _LOGGER.error("处理设备事件失败: %s", e)

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
