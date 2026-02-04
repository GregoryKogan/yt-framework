from yt_framework.core.pipeline import DebugContext
from yt_framework.core.stage import BaseStage


class CreateUsersStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        self.logger.info("Creating users table...")

        users = [
            {"user_id": 1, "name": "Alice", "email": "alice@example.com"},
            {"user_id": 2, "name": "Bob", "email": "bob@example.com"},
            {"user_id": 3, "name": "Charlie", "email": "charlie@example.com"},
        ]

        self.deps.yt_client.write_table(
            table_path=self.config.client.output_table,
            rows=users,
        )

        self.logger.info(f"Created users table: {self.config.client.output_table}")

        # Pass table path to next stages via debug context
        debug["users_table"] = self.config.client.output_table
        debug["users_count"] = len(users)

        return debug
