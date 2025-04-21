
import time

class PyRedisStore:
    def __init__(self):
        """Initializes the main data store and expiration tracking."""
        self._data = {}
        self._expirations = {} # Stores key -> expiry_timestamp
        print("PyRedisStore initialized.")

    def _check_expiry(self, key):
        """Checks if a key has expired and deletes it if necessary."""
        if key in self._expirations:
            if self._expirations[key] < time.time():
                print(f"Key '{key}' expired, deleting.")
                if key in self._data:
                    del self._data[key]
                del self._expirations[key]
                return True
        return False

    def command_set(self, key, value, expire_ms=None):
        """Sets a key-value pair. Optionally sets an expiration time in milliseconds."""
        print(f"Executing: SET {key} {value}" + (f" EX {expire_ms}" if expire_ms else ""))
        self._data[key] = value
        if expire_ms is not None:
            try:
                expire_seconds = int(expire_ms) / 1000.0
                expiry_timestamp = time.time() + expire_seconds
                self._expirations[key] = expiry_timestamp
                print(f"Key '{key}' will expire at timestamp {expiry_timestamp}")
            except ValueError:
                 print(f"Error: Invalid expiration time '{expire_ms}'. SET failed.")
                 if key in self._data: del self._data[key]
                 if key in self._expirations: del self._expirations[key]
                 return "ERROR: Invalid expiration time format."
        elif key in self._expirations:
             # If SET is used without expiry on a key that had one, remove the expiry
             del self._expirations[key]
             print(f"Removed expiration for key '{key}'")
        return "OK"

    def command_get(self, key):
        """Gets the value associated with a key."""
        print(f"Executing: GET {key}")
        if self._check_expiry(key):
            return None

        value = self._data.get(key, None)
        print(f"Retrieved: {value}")
        return value

    def command_del(self, *keys):
        """Deletes one or more keys."""
        print(f"Executing: DEL {' '.join(keys)}")
        deleted_count = 0
        for key in keys:
            expired = self._check_expiry(key)
            if key in self._data:
                del self._data[key]
                if key in self._expirations:
                    del self._expirations[key]
                deleted_count += 1
                print(f"Deleted key '{key}'")
            elif expired:
                print(f"Key '{key}' expired just now.")
            else:
                print(f"Key '{key}' not found for deletion.")
        return deleted_count

    def command_ttl(self, key):
        """Returns the remaining time to live of a key that has a timeout."""
        print(f"Executing: TTL {key}")
        if key not in self._data:
            print(f"Key '{key}' does not exist.")
            return -2

        if self._check_expiry(key):
             print(f"Key '{key}' expired just now.")
             return -2

        if key in self._expirations:
            remaining_time = self._expirations[key] - time.time()
            if remaining_time > 0:
                print(f"Key '{key}' has {int(remaining_time)} seconds remaining.")
                return int(remaining_time)
            else:
                 print(f"Key '{key}' expiration time is in the past (but not yet cleaned).")
                 return -2
        else:
            print(f"Key '{key}' has no expiration set.")
            return -1

    def command_expire(self, key, seconds):
        """Sets an expiration time on a key in seconds."""
        print(f"Executing: EXPIRE {key} {seconds}")
        if key not in self._data:
            print(f"Key '{key}' does not exist. Cannot set expiry.")
            return 0

        if self._check_expiry(key):
            print(f"Key '{key}' expired just before EXPIRE command.")
            return 0

        try:
            expire_seconds = int(seconds)
            if expire_seconds <= 0:
                print(f"Expiration seconds must be positive. Removing expiry for '{key}' if it exists.")
                if key in self._expirations:
                    del self._expirations[key]
                return 1
            else:
                expiry_timestamp = time.time() + expire_seconds
                self._expirations[key] = expiry_timestamp
                print(f"Set expiration for key '{key}' to {expire_seconds} seconds from now (timestamp: {expiry_timestamp}).")
                return 1
        except ValueError:
            print(f"Error: Invalid seconds value '{seconds}'.")
            return 0

if __name__ == "__main__":
    store = PyRedisStore()

    print("\n--- Testing Basic Commands ---")
    print(f"SET name Johan: {store.command_set('name', 'Johan')}")
    print(f"GET name: {store.command_get('name')}")
    print(f"SET age 23: {store.command_set('age', '23')}")
    print(f"GET age: {store.command_get('age')}")
    print(f"GET non_existent: {store.command_get('non_existent')}")
    print(f"DEL age: {store.command_del('age')}")
    print(f"GET age: {store.command_get('age')}")
    print(f"DEL name non_existent: {store.command_del('name', 'non_existent')}")
    print(f"GET name: {store.command_get('name')}")

    print("\n--- Testing Expiration ---")
    # Set with expiration (e.g. 2000ms == 2 seconds)
    print(f"SET temp_key temp_value EX 2000: {store.command_set('temp_key', 'temp_value', expire_ms='2000')}")
    print(f"GET temp_key (should exist): {store.command_get('temp_key')}")
    print(f"TTL temp_key (should be > 0): {store.command_ttl('temp_key')}")
    print("Waiting for 3 seconds...")
    time.sleep(3)
    print(f"GET temp_key (should be None): {store.command_get('temp_key')}")
    print(f"TTL temp_key (should be -2): {store.command_ttl('temp_key')}")

    print("\n--- Testing EXPIRE command ---")
    print(f"SET persistent_key data: {store.command_set('persistent_key', 'data')}")
    print(f"TTL persistent_key (should be -1): {store.command_ttl('persistent_key')}")
    print(f"EXPIRE persistent_key 2: {store.command_expire('persistent_key', '2')}")
    print(f"TTL persistent_key (should be > 0): {store.command_ttl('persistent_key')}")
    print("Waiting for 3 seconds...")
    time.sleep(3)
    print(f"GET persistent_key (should be None): {store.command_get('persistent_key')}")
    print(f"TTL persistent_key (should be -2): {store.command_ttl('persistent_key')}")

    print("\n--- Testing DEL on expired key ---")
    print(f"SET short_lived short_data EX 1000: {store.command_set('short_lived', 'short_data', expire_ms='1000')}")
    print("Waiting 2 seconds...")
    time.sleep(2)
    print(f"DEL short_lived: {store.command_del('short_lived')}")
