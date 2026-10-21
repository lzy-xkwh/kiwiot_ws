import voluptuous as vol
import aiohttp
import asyncio
from homeassistant import config_entries
from typing import Any, Dict, Optional
from .const import DOMAIN, CONF_IDENTIFIER, CONF_CREDENTIAL, CONF_CLIENT_ID, CONF_IGNORE_SSL, BASE_URL

class KiwiOTConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 1.1
    async def async_step_user(self, user_input: Optional[Dict[str, Any]] = None):
        errors = {}

        if user_input is not None:
            # 校验用户输入
            identifier = user_input.get(CONF_IDENTIFIER)
            credential = user_input.get(CONF_CREDENTIAL)
            client_id = user_input.get(CONF_CLIENT_ID)
            
            #放宽手机号格式校验
            # if not isinstance(identifier, str) or not identifier.startswith("+86") or len(identifier) != 14:
            #     errors["identifier"] = "identifier_invalid_format"
            if not isinstance(identifier, str):
                 errors["identifier"] = "identifier_invalid_format"
            elif not all([identifier, credential, client_id]):
                errors["base"] = "missing_fields"
            else:
                return self.async_create_entry(title="KiwiOT", data=user_input)

        # 构建配置表单
        data_schema = vol.Schema({
            vol.Required(CONF_IDENTIFIER): str,
            vol.Required(CONF_CREDENTIAL): str,
            vol.Required(CONF_CLIENT_ID): str,
            vol.Optional(CONF_IGNORE_SSL, default=False): bool,
        })

        return self.async_show_form(
            step_id="user",
            data_schema=data_schema,
            errors=errors,
        )

    async def async_step_reauth(self, entry_data: Dict[str, Any]):
        """Start reauthentication for an existing config entry."""
        self._reauth_entry = self.hass.config_entries.async_get_entry(
            self.context["entry_id"]
        )
        if self._reauth_entry is None:
            return self.async_abort(reason="reauth_entry_not_found")
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
        self, user_input: Optional[Dict[str, Any]] = None
    ):
        """Validate new credentials before storing them."""
        errors = {}
        if user_input is not None:
            identifier = self._reauth_entry.data[CONF_IDENTIFIER]
            credential = user_input[CONF_CREDENTIAL]
            client_id = user_input[CONF_CLIENT_ID]
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"{BASE_URL}/restapi/auth/tokens",
                        headers={"X-Kiwik-Client-Id": client_id},
                        json={
                            "identifier": identifier,
                            "credential": credential,
                            "auth_type": "password",
                        },
                        timeout=aiohttp.ClientTimeout(total=15),
                    ) as response:
                        if response.status == 201:
                            return self.async_update_reload_and_abort(
                                self._reauth_entry,
                                data_updates={
                                    CONF_CREDENTIAL: credential,
                                    CONF_CLIENT_ID: client_id,
                                },
                            )
                        errors["base"] = "invalid_auth"
            except (aiohttp.ClientError, asyncio.TimeoutError):
                errors["base"] = "cannot_connect"

        schema = vol.Schema(
            {
                vol.Required(CONF_CREDENTIAL): str,
                vol.Required(
                    CONF_CLIENT_ID,
                    default=self._reauth_entry.data.get(CONF_CLIENT_ID, ""),
                ): str,
            }
        )
        return self.async_show_form(
            step_id="reauth_confirm", data_schema=schema, errors=errors
        )
