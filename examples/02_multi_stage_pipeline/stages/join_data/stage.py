from yt_framework.core.pipeline import DebugContext
from yt_framework.core.stage import BaseStage


class JoinDataStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        self.logger.info("Joining users and orders tables...")

        # Get table paths from previous stages or config
        users_table = debug.get("users_table", self.config.client.users_table)
        orders_table = debug.get("orders_table", self.config.client.orders_table)

        self.logger.info(f"Users table: {users_table}")
        self.logger.info(f"Orders table: {orders_table}")

        self.deps.yt_client.join_tables(
            left_table=orders_table,
            right_table=users_table,
            output_table=self.config.client.output_table,
            on="user_id",
            how="left",
            select_columns=[
                "a.order_id",
                "a.user_id",
                "a.product",
                "a.amount",
                "b.name",
                "b.email",
            ],
        )

        row_count = self.deps.yt_client.row_count(self.config.client.output_table)
        self.logger.info(
            f"Joined table has {row_count} rows: {self.config.client.output_table}"
        )

        debug["final_table"] = self.config.client.output_table
        debug["final_row_count"] = row_count

        return debug
