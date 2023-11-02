import datetime

from motor import motor_asyncio
from pymongo import errors
import sys
import bson

from src.logger.logger import tb_logger


class Mongo:
    """Class used to connect to MongoDB, manage and aggregate documents in the collection"""
    def __init__(
            self,
            mongo_username: str,
            mongo_password: str,
            mongo_cluster: str,
            database_name: str,
            collection_name: str,
    ) -> None:
        """

        :param mongo_username: username of mongodb user.
        :param mongo_password: password of mongo
        :param mongo_cluster: usable mongodb cluster name
        :param database_name: usable database name.
        :param collection_name: usable collection name.
        """
        self.mongo_username = mongo_username
        self.mongo_password = mongo_password
        self.mongo_cluster = mongo_cluster
        self.client = self.connect_to_db()
        self.collection = self.client[database_name][collection_name]

    def connect_to_db(self) -> motor_asyncio.AsyncIOMotorClient:
        """
        Method user to connect to DB.
        :return: Motor Async Mongo Client (object)
        """
        try:
            client = motor_asyncio.AsyncIOMotorClient(
                f"mongodb+srv://{self.mongo_username}:{self.mongo_password}@{self.mongo_cluster}"
                f"/?retryWrites=true&w=majority"
            )
            tb_logger.log_info("MongoDB connected.")
        except errors.PyMongoError as e:
            tb_logger.log_info("Не удалось подключиться к базе данных:", e)
            sys.exit(1)
        return client

    async def check_collection(self) -> int:
        """
        Check if collection named self.collection_name exists and not empty
        :return: Amount documents in the collection
        """
        documents_amount = await self.collection.count_documents({})
        tb_logger.log_info("Total documents amount:", documents_amount)
        return documents_amount

    async def import_bson_to_db(self, path_to_bson: str) -> bool:
        """
        Import data from bson file to MongoDB
        :param path_to_bson: path to .bson file from root directory
        :return: True if success, False if failure
        """
        try:
            async with open(path_to_bson, "rb") as f:
                data = bson.decode_file_iter(f)
                result = await self.collection.insert_many(data)
                tb_logger.log_info(f"Inserted {len(result.inserted_ids)} docs.")
                return True
        except FileNotFoundError as e:
            tb_logger.log_info("BSON файл не найден:", e)
        except Exception as e:
            tb_logger.log_info("Произошла ошибка:", e)
        return False

    async def create_index(self, field: str = "dt") -> None:
        """
        Create index in the collection by key name.
        :param field: key by which index must be created
        :return:
        """
        indexes = self.collection.list_indexes()
        index_exists = any(field in ind["key"] for ind in await indexes.to_list(None))

        if not index_exists:
            try:
                await self.collection.create_index(field)
            except Exception as e:
                tb_logger.log_info(f"Не удалось создать индекс для ключа {field}:", e)

    async def get_data_from_db(self, input_data: dict[str, str]) -> dict[str, list[int] | str]:
        """
        Method used to get data from DB, aggregate it and return as dict
        :param input_data: input data as dict with following structure:
        {
            "dt_from": "2022-09-01T00:00:00",
            "dt_upto": "2022-12-31T23:59:00",
            "group_type": "month"
        }
        :return: dict in format:
        {"dataset": [5906586, 5515874, 5889803, 6092634],
        "labels": ["2022-09-01T00:00:00", "2022-10-01T00:00:00", "2022-11-01T00:00:00", "2022-12-01T00:00:00"]}
        """
        # this dict used to format "labels" in the returning dict
        group_format = {
            "hour": "%Y-%m-%d-%H",
            "day": "%Y-%m-%d",
            "month": "%Y-%m",
        }

        start_date = datetime.datetime.fromisoformat(input_data["dt_from"])
        end_date = datetime.datetime.fromisoformat(input_data["dt_upto"])
        date_format = group_format[input_data["group_type"]]

        # pipeline with stages to aggregate data
        pipeline = [
            {"$match": {"dt": {"$gte": start_date, "$lte": end_date}}},
            {
                "$group": {
                    "_id": {
                        "$dateToString": {
                            "format": date_format,
                            "date": "$dt"
                        },
                    },
                    "summary": {"$sum": "$value"},
                },
            },
            {"$sort": {"_id": 1}},
        ]
        aggregated = self.collection.aggregate(pipeline=pipeline)
        result = {"dataset": [], "labels": []}
        # convert MongoDB cursor object to dict
        async for doc in aggregated:
            date_as_obj = datetime.datetime.strptime(doc["_id"], date_format)
            result["dataset"].append(doc["summary"])
            result["labels"].append(date_as_obj.isoformat())
        return result
