"""
Utilities for sending Slack messages.

All integrations go through the Metaflow app within the Move Slack Workspace - https://api.slack.com/apps/A04EVSRFKU6

To help distinguish between messages originating from user code versus system-managed infrastructure, there are personas
for `ocpipes` and `Metaflow`. When messages are sent, the bot username and avatar will be customized accordingly.
"""

from __future__ import annotations

import base64
import uuid
from dataclasses import dataclass
from enum import Enum, unique
from typing import Any

from metaflow import current
from metaflow.cards import Image
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from ocpipes import logger
from ocpipes.secrets import SlackConnectionSecrets

client = WebClient(token=SlackConnectionSecrets().token)


@unique
class SlackNotificationType(Enum):
    ALERT = "alert"
    IMAGE = "image"
    TABLE = "table"


@unique
class SlackPersona(Enum):
    """
    The bot username.
    """

    OCPIPES = "ocpipes"
    METAFLOW = "Metaflow"


@unique
class SlackPersonaIconEmoji(Enum):
    """
    The icon to use for the bot avatar, can use any emojis available in the Move workspace.
    """

    OCPIPES = ":pipe:"
    METAFLOW = ":metaflow:"


@dataclass
class SlackNotification:
    """
    Enforce consistency in the structure of slack notifications.

    Args:
        channel (str): The channel or username to directly message. If the channel is private, the Metaflow slack app
            must first be added to it. E.g. a channel `#data-science-metaflow` or a username like `@russell.brooks`.
        message (str, optional): Text content within the notification. Required when sending a notification of type
            SlackNotificationType.ALERT.
        notify (str | list[str], optional): A username or list of usernames to include in an `@mention` block of the
            message for notifications. Defaults to None.
        thread_ts (str, optional): Provide another message's `ts` value to make this message a reply in a thread. Avoid
            using a reply's `ts`; use it's parent instead. Defaults to None.
        notification_type (SlackNotificationType): The type of notification content to be formatted. Defaults to
            SlackNotificationType.ALERT.
        persona (SlackPersona): The bot username. Defaults to SlackPersona.OCPIPES
        emoji (SlackPersonaIconEmoji): The icon to use for the bot avatar. Defaults to SlackPersonaIconEmoji.OCPIPES.
        image (Image | list[Image], optional): A Metaflow card `Image` or list of `Image`. Required when sending a
            notification of type SlackNotificationType.IMAGE. Defaults to None.
    """

    channel: str
    message: str | None
    notify: str | list[str] | None = None
    thread_ts: str | None = None
    notification_type: SlackNotificationType = SlackNotificationType.ALERT
    persona: SlackPersona = SlackPersona.OCPIPES
    emoji: SlackPersonaIconEmoji = SlackPersonaIconEmoji.OCPIPES

    image: Image | list[Image] | None = None

    def __post_init__(self) -> None:
        self.notify = [self.notify] if isinstance(self.notify, str) else self.notify
        self.image = [self.image] if isinstance(self.image, Image) else self.image

        # tbd: guardrail certain channels for dev vs prod?
        assert self.channel and isinstance(self.channel, str)
        assert self.notify is None or all(
            (user and isinstance(user, str)) for user in self.notify
        )
        assert self.thread_ts is None or (
            self.thread_ts and isinstance(self.thread_ts, str)
        )
        assert isinstance(self.notification_type, SlackNotificationType)
        assert isinstance(self.persona, SlackPersona)
        assert isinstance(self.emoji, SlackPersonaIconEmoji)

        if self.notification_type == SlackNotificationType.ALERT:
            assert self.message and isinstance(self.message, str)
            assert self.image is None

        elif self.notification_type == SlackNotificationType.IMAGE:
            assert self.image and all(isinstance(image, Image) for image in self.image)
            assert self.message is None or (
                self.message and isinstance(self.message, str)
            )

        elif self.notification_type == SlackNotificationType.TABLE:
            raise NotImplementedError

    def send_slack_msg(self) -> None:
        payload = None

        if self.notification_type == SlackNotificationType.ALERT:
            payload = self._slack_alert_payload()

        elif self.notification_type == SlackNotificationType.IMAGE:
            # Can't use v2 yet – see commentary below. In the meantime, we post a message with the blocks followed
            # immediately by another post that uploads the image.

            # permalinks = self._upload_images_v2()
            # payload = self._slack_image_payload(permalinks)
            payload = self._slack_image_payload()

        elif self.notification_type == SlackNotificationType.TABLE:
            payload = self._slack_table_payload()

        if payload:
            try:
                client.chat_postMessage(**payload)

                # Can remove this after swapping to v2 and the images are within the message blocks.
                if self.notification_type == SlackNotificationType.IMAGE:
                    self._upload_images()

            except SlackApiError as e:
                response = e.response
                logger.error(f"SlackApiError: {response['error']}")

    def _get_notify_block(self) -> list[dict[str, Any]] | None:
        """
        If individuals to notify are specified, add a block for the `@mentions` to highlight them in the message.
        """
        blocks = None
        if self.notify:
            # fmt: off
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": ":mega: Notifying " + " ".join([f"<{user}>" for user in self.notify])
                    }
                },
                {
                    "type": "divider"
                },
            ]
            # fmt: on
        return blocks

    def _get_metaflow_block(self) -> list[dict[str, Any]] | None:
        """
        Infer if the code is being executed within a Metaflow run, and if so add a block with general information about
        the environment and execution metadata, including a link to the UI.
        """
        blocks = None
        if current.is_running_flow:
            env = "prod" if getattr(current, "is_production", False) else "dev"
            ui_link = f"https://metaflow.opcity.com/{current.pathspec}"
            # fmt: off
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": ":metaflow: Environment details"
                    },
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Env:* {env}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*User:* {current.username}"
                        },
                    ] + ([
                        # Add @project metadata when available.
                        {
                            "type": "mrkdwn",
                            "text": f"*Project:* {current.project_name}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Project Branch:* {current.branch_name}"
                        }
                    ] if getattr(current, "project_name", None) else [])
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f" :information_source: *<{ui_link}>*",
                        }
                    ],
                },
                {
                    "type": "divider"
                },
            ]
            # fmt: on
        return blocks

    def _get_message_block(self) -> list[dict[str, Any] | None]:
        blocks = None
        if self.message:
            # fmt: off
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": self.message
                    }
                }
            ]
            # fmt: on
        return blocks

    def _upload_images(self) -> None:
        if self.image:
            # If executed within a flow, generate the filenames from the pathspec, otherwise a random uuid.
            for count, image in enumerate(self.image, start=1):
                filename = (
                    f'img{count}-{current.pathspec.replace("/", "-")}'
                    if current.is_running_flow
                    else uuid.uuid1().hex[:8]
                )
                client.files_upload(
                    filename=filename,
                    content=_extract_image_bytes(image),
                    channels=self.channel,
                )

    def _upload_images_v2(self) -> dict[str, str]:
        """
        The v2 endpoint is recommended and supports multiple files at once, but does not support specifying channels or
        users by name, only id, so this is on the backburner till the API gets updated. Another issue is that uploaded
        files can only be be correctly linked in channels during the initial upload due to our Org settings.

        We'll also want to use the v2 approach with blocks so we can utilize the personas.
        """
        # Extract the image bytes and upload each to slack. Once uploaded, the images are attached via blocks.
        # If channels are passed to `files_upload()` directly, it will be shared but without the ability to change
        # the username and icon of the bot, or to use blocks for richer message content.
        permalinks = {}
        if self.image:
            # Construct the upload payload for multiple images at once and use the new v2 endpoint for performance.
            # If executed within a flow, generate the filenames from the pathspec, otherwise a random uuid.
            file_uploads = []
            for count, image in enumerate(self.image, start=1):
                filename = (
                    f'img{count}-{current.pathspec.replace("/", "-")}'
                    if current.is_running_flow
                    else uuid.uuid1().hex[:8]
                )
                file_uploads.append(
                    {"filename": filename, "content": _extract_image_bytes(image)}
                )

            response = client.files_upload_v2(file_uploads=file_uploads)
            for file in response.get("files"):
                permalinks[file.get("name")] = file.get("permalink")

        return permalinks

    def _get_image_block_v2(self, permalinks: dict[str, str]) -> list[dict[str, Any]]:
        # fmt: off
        blocks = [
            {
                "type": "image",
                "title": {
                    "type": "plain_text",
                    "text": filename
                },
                "image_url": permalink,
                "alt_text": filename
            }
            for filename, permalink in permalinks.items()
        ]
        # fmt: on
        return blocks

    def _slack_alert_payload(self) -> dict[str, Any] | None:
        blocks = []

        notify_block = self._get_notify_block()
        if notify_block:
            blocks += notify_block

        metaflow_block = self._get_metaflow_block()
        if metaflow_block:
            blocks += metaflow_block

        message_block = self._get_message_block()
        if message_block:
            blocks += message_block

        # When `blocks` are used, `text` becomes a fallback message for limited devices.
        payload = {
            "thread_ts": self.thread_ts,
            "channel": self.channel,
            "username": self.persona.value,
            "icon_emoji": self.emoji.value,
            "blocks": blocks,
            "text": self.message,
        }

        return payload

    def _slack_image_payload(self) -> dict[str, Any] | None:
        blocks = []

        notify_block = self._get_notify_block()
        if notify_block:
            blocks += notify_block

        metaflow_block = self._get_metaflow_block()
        if metaflow_block:
            blocks += metaflow_block

        message_block = self._get_message_block()
        if message_block:
            blocks += message_block

        # Omitting image block since uploaded files can only be be correctly linked in channels on initial upload.
        # Instead we simply post the blocks and upload the image with two requests back-to-back. This also means we
        # can't use personas, so we'll force everything to use the default app profile.

        payload = {
            "thread_ts": self.thread_ts,
            "channel": self.channel,
            # "username": self.persona.value,
            # "icon_emoji": self.emoji.value,
            "blocks": blocks,
            "text": self.message,
        }

        return payload

    def _slack_image_payload_v2(
        self, permalinks: dict[str, str]
    ) -> dict[str, Any] | None:
        blocks = []

        notify_block = self._get_notify_block()
        if notify_block:
            blocks += notify_block

        metaflow_block = self._get_metaflow_block()
        if metaflow_block:
            blocks += metaflow_block

        message_block = self._get_message_block()
        if message_block:
            blocks += message_block

        image_block = self._get_image_block_v2(permalinks)
        if image_block:
            blocks += image_block

        payload = {
            "thread_ts": self.thread_ts,
            "channel": self.channel,
            "username": self.persona.value,
            "icon_emoji": self.emoji.value,
            "blocks": blocks,
            "text": self.message,
        }

        return payload

    def _slack_table_payload(self) -> dict[str, Any] | None:
        # Need to parse out a dataframe into text
        # Should guardrail for large tables/dfs
        # Might be able to use `df.to_markdown()`, Tabulate, rich, some other package
        # e.g. send training metrics, data quality stats if below threshold, AB test results, etc.
        raise NotImplementedError


def _extract_image_bytes(image: Image) -> bytes:
    # Metaflow card images are serialized from bytes to a base64 encoded utf8 string, that's prefixed with metadata and
    # stored in a dict. We can extract the image by decoding the base64 bytes.
    base64_bytes = image.render()["src"].split(", ")[1].encode("utf8")
    return base64.b64decode(base64_bytes)


def slack_message(
    message: str,
    channel: str | list[str],
    notify: str | list[str] | None = None,
    thread_ts: str | None = None,
    persona: SlackPersona = SlackPersona.OCPIPES,
    emoji: SlackPersonaIconEmoji = SlackPersonaIconEmoji.OCPIPES,
) -> None:
    """
    Sends a slack message to a channel or private DM, with the option to @mention people, alongside runtime environment
    details.

    Args:
        message (str): Text content within the notification.
        channel (str | list[str]): The channels or usernames to directly message. If the channel is private, the
            Metaflow slack app must first be added to it. Examples could be a channel like `#data-science-metaflow`, a
            username like `@russell.brooks`, or a list of multiple.
        notify (str | list[str] | None): A username or list of usernames to include in an `@mention` block of the
            message for notifications. Defaults to None.
        thread_ts (str, optional): Provide another message's `ts` value to make this message a reply in a thread. Avoid
            using a reply's `ts`; use it's parent instead. Defaults to None.
        persona (SlackPersona, optional): The bot username. Defaults to SlackPersona.OCPIPES
        emoji (SlackPersonaIconEmoji, optional): The icon to use for the bot avatar. Defaults to
            SlackPersonaIconEmoji.OCPIPES.
    """
    channel = [channel] if isinstance(channel, str) else channel
    for channel_or_username in channel:
        notification = SlackNotification(
            message=message,
            channel=channel_or_username,
            notify=notify,
            thread_ts=thread_ts,
            notification_type=SlackNotificationType.ALERT,
            persona=persona,
            emoji=emoji,
        )
        notification.send_slack_msg()


def slack_image(
    image: Image | list[Image],
    channel: str | list[str],
    message: str | None = None,
    notify: str | list[str] | None = None,
    thread_ts: str | None = None,
    persona: SlackPersona = SlackPersona.OCPIPES,
    emoji: SlackPersonaIconEmoji = SlackPersonaIconEmoji.OCPIPES,
) -> None:
    """
    Sends a slack message with an image.

    Reuses the Metaflow card `Image` for convenience in existing flows, plus it handles many common cases such as
    supporting PNG/JPG/GIF bytes, PIL images, or Matplotlib figures.

    Note: until we can refactor for the v2 file upload api alongside blocks, image messages will not utilize personas.

    Args:
        image (Image | list[Image]): The Metaflow card `Image` objects to upload and share in a message.
        channel (str | list[str]): The channels or usernames to directly message. If the channel is private, the
            Metaflow slack app must first be added to it. Examples could be a channel like `#data-science-metaflow`, a
            username like `@russell.brooks`, or a list of multiple.
        message (str, optional): Text content within the notification. Defaults to None.
        notify (str | list[str] | None): A username or list of usernames to include in an `@mention` block of the
            message for notifications. Defaults to None.
        thread_ts (str, optional): Provide another message's `ts` value to make this message a reply in a thread. Avoid
            using a reply's `ts`; use it's parent instead. Defaults to None.
        persona (SlackPersona, optional): The bot username. Defaults to SlackPersona.OCPIPES
        emoji (SlackPersonaIconEmoji, optional): The icon to use for the bot avatar. Defaults to
            SlackPersonaIconEmoji.OCPIPES.
    """

    channel = [channel] if isinstance(channel, str) else channel
    for channel_or_username in channel:
        notification = SlackNotification(
            message=message,
            channel=channel_or_username,
            notify=notify,
            thread_ts=thread_ts,
            notification_type=SlackNotificationType.IMAGE,
            persona=persona,
            emoji=emoji,
            image=image,
        )
        notification.send_slack_msg()


def slack_table():
    raise NotImplementedError
