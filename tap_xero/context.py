import singer
from singer import state as st_
from .client import XeroClient


class Context():
    def __init__(self, config, state, catalog, config_path):
        self.config = config
        self.config_path = config_path
        self.state = state
        self.catalog = catalog
        self.client = XeroClient(config)

    def refresh_credentials(self):
        self.client.refresh_credentials(self.config, self.config_path)

    def check_platform_access(self):
        self.client.check_platform_access(self.config, self.config_path)

    def get_bookmark(self, path):
        return st_.get_bookmark(self.state, *path)

    def set_bookmark(self, path, val):
        st_.set_bookmark(self.state, path[0], path[1], val)

    def get_offset(self, path):
        off = st_.get_offset(self.state, path[0])
        return (off or {}).get(path[1])

    def set_offset(self, path, val):
        st_.set_offset(self.state, path[0], path[1], val)

    def clear_offsets(self, tap_stream_id):
        st_.clear_offset(self.state, tap_stream_id)

    def update_start_date_bookmark(self, path):
        val = self.get_bookmark(path)
        if not val:
            val = self.config["start_date"]
            self.set_bookmark(path, val)
        return val

    def write_state(self):
        singer.write_state(self.state)
