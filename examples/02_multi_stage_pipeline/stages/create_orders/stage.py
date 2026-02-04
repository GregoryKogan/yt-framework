from yt_framework.core.pipeline import DebugContext
from yt_framework.core.stage import BaseStage


class CreateOrdersStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        self.logger.info("Creating orders table...")

        # Access data from previous stage
        if "users_count" in debug:
            self.logger.info(f"Previous stage created {debug['users_count']} users")

        orders = [
            {"order_id": 101, "user_id": 1, "product": "Laptop", "amount": 999.99},
            {"order_id": 102, "user_id": 1, "product": "Mouse", "amount": 29.99},
            {"order_id": 103, "user_id": 2, "product": "Keyboard", "amount": 79.99},
            {"order_id": 104, "user_id": 3, "product": "Monitor", "amount": 299.99},
        ]

        self.deps.yt_client.write_table(
            table_path=self.config.client.output_table,
            rows=orders,
        )

        self.logger.info(f"Created orders table: {self.config.client.output_table}")

        # Pass table path to next stage
        debug["orders_table"] = self.config.client.output_table
        debug["orders_count"] = len(orders)

        return debug
