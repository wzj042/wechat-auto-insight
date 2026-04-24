"""异常告警邮件服务。

通过标准库 smtplib 发送告警邮件，配置优先从环境变量读取。
不引入额外第三方依赖。
"""

from __future__ import annotations

import os
import smtplib
import ssl
import traceback
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

from .settings import (
    ALERT_FROM,
    ALERT_SMTP_HOST,
    ALERT_SMTP_PASSWORD,
    ALERT_SMTP_PORT,
    ALERT_SMTP_USER,
    ALERT_TO,
)


def _is_alert_configured() -> bool:
    """检查是否具备发送告警邮件的最小配置。"""
    return bool(ALERT_SMTP_HOST and ALERT_SMTP_USER and ALERT_SMTP_PASSWORD and ALERT_FROM and ALERT_TO)


def _build_alert_recipients() -> list[str]:
    """解析环境变量中的收件人列表，支持逗号/分号分隔。"""
    raw = (ALERT_TO or "").strip()
    if not raw:
        return []
    return [addr.strip() for addr in raw.replace(";", ",").split(",") if addr.strip()]


def send_alert_email(subject: str, body: str, *, html: bool = False) -> dict[str, Any]:
    """发送一封告警邮件。

    如果未配置邮箱参数，则直接返回未发送状态，不抛异常。
    返回包含 sent / error / detail 的字典，便于调用者记录日志。
    """
    if not _is_alert_configured():
        return {"sent": False, "error": "邮件告警未配置", "detail": ""}

    recipients = _build_alert_recipients()
    if not recipients:
        return {"sent": False, "error": "未配置有效的告警收件人", "detail": ""}

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = ALERT_FROM
    msg["To"] = ", ".join(recipients)

    if html:
        msg.attach(MIMEText(body, "html", "utf-8"))
    else:
        msg.attach(MIMEText(body, "plain", "utf-8"))

    use_ssl = ALERT_SMTP_PORT in (465, 587)
    try:
        context = ssl.create_default_context()
        if use_ssl:
            server = smtplib.SMTP_SSL(ALERT_SMTP_HOST, ALERT_SMTP_PORT, context=context, timeout=15)
        else:
            server = smtplib.SMTP(ALERT_SMTP_HOST, ALERT_SMTP_PORT, timeout=15)
            server.starttls(context=context)

        server.login(ALERT_SMTP_USER, ALERT_SMTP_PASSWORD)
        server.sendmail(ALERT_FROM, recipients, msg.as_string())
        server.quit()
        return {"sent": True, "error": "", "detail": f"已发送至 {len(recipients)} 位收件人"}
    except Exception as exc:
        return {"sent": False, "error": str(exc), "detail": traceback.format_exc()}


def maybe_send_alert(subject: str, body: str, *, html: bool = False) -> None:
    """尝试发送告警邮件，并将结果打印到 stdout。"""
    result = send_alert_email(subject, body, html=html)
    if result["sent"]:
        print(f"[AlertSent] {result['detail']}: {subject}", flush=True)
    else:
        print(f"[AlertSkipped] {result['error']}: {subject}", flush=True)
