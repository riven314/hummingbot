from typing import TYPE_CHECKING, Callable

import pandas as pd
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from hummingbot.core.utils.async_call_scheduler import AsyncCallScheduler
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.notifier.notifier_base import NotifierBase

if TYPE_CHECKING:
    from hummingbot.client.hummingbot_application import HummingbotApplication


def authorized_only(handler: Callable) -> Callable:
    """
    Decorator to restrict access to authorized users only
    """

    async def wrapper(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.effective_chat and str(update.effective_chat.id) == self._chat_id:
            return await handler(self, update, context)
        else:
            # Log unauthorized access
            self.logger().warning(
                f"Unauthorized access denied for {update.effective_chat.id if update.effective_chat else 'Unknown'}"
            )

    return wrapper


class TelegramNotifier(NotifierBase):
    def __init__(self, token: str, chat_id: str, hb: "HummingbotApplication") -> None:
        super().__init__()
        self._token = token
        self._chat_id = chat_id
        self._hb = hb
        self._application = None

    def __eq__(self, other) -> bool:
        return (
            isinstance(other, self.__class__)
            and self._token == other._token
            and self._chat_id == other._chat_id
            and id(self._hb) == id(other._hb)
        )

    def start(self) -> None:
        if self._started:
            self.logger().info("Telegram notifier already started.")
            return

        self._application = Application.builder().token(self._token).build()
        assert self._application is not None

        # accept the following commands from the user (have to prefix with /, e.g. /start)
        self._application.add_handler(CommandHandler("start", self.handler))
        self._application.add_handler(CommandHandler("stop", self.handler))
        self._application.add_handler(CommandHandler("status", self.handler))
        self._application.add_handler(CommandHandler("history", self.handler))
        self._application.add_handler(CommandHandler("config", self.handler))

        # Use safe_ensure_future to handle the async initialization
        safe_ensure_future(self._initialize_application(), loop=self._ev_loop)

        super().start()
        self.logger().info("Telegram notifier started.")
        self._started = True

    async def _initialize_application(self) -> None:
        try:
            await self._application.initialize()
            await self._application.start()
            await self._application.updater.start_polling(allowed_updates=["message"])
            self.logger().info("Telegram application fully initialized.")
        except Exception as e:
            self.logger().error(f"Error initializing Telegram application: {e}", exc_info=True)
            self._started = False

    def stop(self) -> None:
        super().stop()
        self.logger().info("Telegram notifier stopping...")
        if self._application and self._application.running:
            # Use safe_ensure_future to handle the async shutdown
            safe_ensure_future(self._shutdown_application(), loop=self._ev_loop)
        self._started = False
        self.logger().info("Telegram notifier stopped.")

    async def _shutdown_application(self) -> None:
        try:
            await self._application.updater.stop()
            await self._application.stop()
            await self._application.shutdown()
            self._application = None
        except Exception as e:
            self.logger().error(f"Error shutting down Telegram application: {e}", exc_info=True)

    @authorized_only
    async def handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await self.handler_loop(update, context)

    async def handler_loop(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        command = update.message.text.split()[0].lower()
        self.logger().info(f"Received command: {command}")

        if command.startswith("/"):
            command = command[1:]

        # Set display options to max, so that telegram does not display truncated data
        pd.set_option("display.max_rows", 500)
        pd.set_option("display.max_columns", 500)
        pd.set_option("display.width", 1000)

        async_scheduler: AsyncCallScheduler = AsyncCallScheduler.shared_instance()
        await async_scheduler.call_async(self._hb._handle_command(command), timeout_seconds=30)

        # Reset to normal, so that pandas's default autodetect width still works
        pd.set_option("display.max_rows", 0)
        pd.set_option("display.max_columns", 0)
        pd.set_option("display.width", 0)

    # @staticmethod
    # def _divide_chunks(arr: List[Any], n: int = 5) -> Generator[List[Any], None, None]:
    #     """Divide a list into chunks of size n"""
    #     for i in range(0, len(arr), n):
    #         yield arr[i : i + n]

    async def _send_message(self, msg: str) -> None:
        if not self._started or self._application is None:
            self.logger().error("Cannot send telegram message: notifier not started")
            return

        try:
            await self._application.bot.send_message(chat_id=self._chat_id, text=msg)
        except Exception as e:
            self.logger().error(f"Error sending telegram message: {e}", exc_info=True)
