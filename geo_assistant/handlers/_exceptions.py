class InvalidTileservTableID(Exception):

    def __init__(self, table_id):
        self.message = (
            f"TableID {table_id} not valid. Please check http://localhost:7800/index.json"
        )
        super().__init__(self.message)