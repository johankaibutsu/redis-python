import time
from collections import deque

class PyRedisStore:
    def __init__(self):
        """Initializes the main data store and expiration tracking."""
        self._data = {}
        self._expirations = {}
        print("PyRedisStore initialized.")

    def _check_expiry(self, key):
        """Checks if a key has expired and deletes it if necessary. Returns True if expired, False otherwise."""
        if key in self._expirations:
            if self._expirations[key] < time.time():
                print(f"Key '{key}' expired, deleting.")
                self._delete_key_internal(key)
                return True
        return False

    def _delete_key_internal(self, key):
        """Internal helper to delete a key and its expiry."""
        deleted = False
        if key in self._data:
            del self._data[key]
            deleted = True
        if key in self._expirations:
            del self._expirations[key]
        return deleted

    def _get_value_or_error(self, key, expected_type=None):
        """Helper to get a value, checking expiry and optionally type."""
        if self._check_expiry(key):
            return None, None

        value = self._data.get(key)
        if value is None:
            return None, None

        if expected_type is not None and not isinstance(value, expected_type):
            error_msg = f"WRONGTYPE Operation against a key holding the wrong kind of value"
            print(f"Error for key '{key}': {error_msg}")
            return None, error_msg

        return value, None

    def command_set(self, key, value, expire_ms=None):
        """Sets a key-value pair (string). Overwrites existing keys of any type."""
        print(f"Executing: SET {key} {value}" + (f" EX {expire_ms}" if expire_ms else ""))

        self._data[key] = str(value)
        if expire_ms is not None:
            try:
                expire_seconds = int(expire_ms) / 1000.0
                if expire_seconds <= 0:
                     print(f"Error: Invalid expiration time '{expire_ms}'. Must be positive.")
                     self._delete_key_internal(key)
                     return "ERROR: Invalid expiration time format."

                expiry_timestamp = time.time() + expire_seconds
                self._expirations[key] = expiry_timestamp
                print(f"Key '{key}' will expire at timestamp {expiry_timestamp}")
            except ValueError:
                 print(f"Error: Invalid expiration time format '{expire_ms}'. SET failed.")
                 self._delete_key_internal(key)
                 return "ERROR: Invalid expiration time format."
        elif key in self._expirations:
             del self._expirations[key]
             print(f"Removed expiration for key '{key}'")

        return "OK"

    def command_get(self, key):
        """Gets the value associated with a key (string)."""
        print(f"Executing: GET {key}")
        value, error = self._get_value_or_error(key, expected_type=str)
        if error:
            return f"ERROR: {error}"
        print(f"Retrieved: {value}")
        return value

    def command_del(self, *keys):
        """Deletes one or more keys."""
        print(f"Executing: DEL {' '.join(keys)}")
        deleted_count = 0
        for key in keys:
             self._check_expiry(key)
             if self._delete_key_internal(key):
                 deleted_count += 1
                 print(f"Deleted key '{key}'")
             else:
                 print(f"Key '{key}' not found for deletion.")
        return deleted_count
    def command_lpush(self, key, *values):
        """Prepends one or multiple values to a list. Creates list if key doesn't exist."""
        print(f"Executing: LPUSH {key} {' '.join(values)}")
        if not values:
             print("Error: LPUSH requires at least one value.")
             return "ERROR: wrong number of arguments for 'lpush' command"
        if self._check_expiry(key):
            self._data[key] = deque(values)
            if key in self._expirations: del self._expirations[key]
            print(f"Created new list for key '{key}' after expiry.")
            return len(values)

        current_value = self._data.get(key)
        if current_value is None:
            self._data[key] = deque(values)
            if key in self._expirations: del self._expirations[key]
            print(f"Created new list for key '{key}'.")
            return len(values)
        elif isinstance(current_value, deque):
            for value in reversed(values):
                current_value.appendleft(value)
            print(f"Prepended {len(values)} values to list '{key}'.")
            return len(current_value)
        else:
            error_msg = "WRONGTYPE Operation against a key holding the wrong kind of value"
            print(f"Error for key '{key}': {error_msg}")
            return f"ERROR: {error_msg}"

    def command_rpush(self, key, *values):
        """Appends one or multiple values to a list. Creates list if key doesn't exist."""
        print(f"Executing: RPUSH {key} {' '.join(values)}")
        if not values:
             print("Error: RPUSH requires at least one value.")
             return "ERROR: wrong number of arguments for 'rpush' command"

        if self._check_expiry(key):
            self._data[key] = deque(values)
            if key in self._expirations: del self._expirations[key]
            print(f"Created new list for key '{key}' after expiry.")
            return len(values)

        current_value = self._data.get(key)
        if current_value is None:
            self._data[key] = deque(values)
            if key in self._expirations: del self._expirations[key]
            print(f"Created new list for key '{key}'.")
            return len(values)
        elif isinstance(current_value, deque):
            for value in values:
                current_value.append(value)
            print(f"Appended {len(values)} values to list '{key}'.")
            return len(current_value)
        else:
            error_msg = "WRONGTYPE Operation against a key holding the wrong kind of value"
            print(f"Error for key '{key}': {error_msg}")
            return f"ERROR: {error_msg}"

    def command_lrange(self, key, start_str, stop_str):
        """Returns a range of elements from a list."""
        print(f"Executing: LRANGE {key} {start_str} {stop_str}")
        try:
            start = int(start_str)
            stop = int(stop_str)
        except ValueError:
            print("Error: start and stop indices must be integers.")
            return "ERROR: value is not an integer or out of range"

        value, error = self._get_value_or_error(key, expected_type=deque)
        if error:
            return f"ERROR: {error}"
        if value is None:
             print(f"List '{key}' not found or expired.")
             return []

        list_len = len(value)
        if stop < 0:
            stop = list_len + stop
        adjusted_stop = stop + 1

        # Handle edge cases and slice calculation carefully
        # Redis LRANGE examples:
        # LRANGE mylist 0 -1 => Get all elements
        # LRANGE mylist 0 0 => Get first element
        # LRANGE mylist -1 -1 => Get last element
        # LRANGE mylist -2 -1 => Get last two elements

        # Python slicing handles most cases naturally:
        # list[0:] gets all from start
        # list[:5] gets first 5 (0 to 4)
        # list[-1:] gets last element
        # list[-2:] gets last two elements

        # Lets convert to Python slice indices
        py_start = start
        py_end = adjusted_stop

        # Handle LRANGE mylist 0 -1 (get all)
        if start == 0 and stop == -1:
             py_end = None

        # Ensure indices are within reasonable bounds for slicing if needed,
        # although Python slicing is quite forgiving.
        # Example: If list has 5 items (len=5, indices 0-4)
        # LRANGE mylist 0 2 => Python slice [0:3] -> items 0, 1, 2
        # LRANGE mylist -2 -1 => Python slice [-2:] -> items 3, 4

        sliced_list = list(value)[py_start:py_end]
        print(f"Retrieved range [{start}:{stop}]: {sliced_list}")
        return sliced_list

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
                removed = 0
                if key in self._expirations:
                    del self._expirations[key]
                    removed = 1
                return removed
            else:
                expiry_timestamp = time.time() + expire_seconds
                self._expirations[key] = expiry_timestamp
                print(f"Set expiration for key '{key}' to {expire_seconds} seconds from now (timestamp: {expiry_timestamp}).")
                return 1 # Expiry set successfully
        except ValueError:
            print(f"Error: Invalid seconds value '{seconds}'.")
            return 0 # Failed to set expiry

if __name__ == "__main__":
    store = PyRedisStore()

    print("\n--- Testing List Commands ---")
    print(f"LPUSH mylist a: {store.command_lpush('mylist', 'a')}") # mylist: ['a']
    print(f"LPUSH mylist b c: {store.command_lpush('mylist', 'b', 'c')}") # mylist: ['c', 'b', 'a']
    print(f"RPUSH mylist d e: {store.command_rpush('mylist', 'd', 'e')}") # mylist: ['c', 'b', 'a', 'd', 'e']
    print(f"LRANGE mylist 0 2: {store.command_lrange('mylist', '0', '2')}") # Should be ['c', 'b', 'a']
    print(f"LRANGE mylist 0 -1: {store.command_lrange('mylist', '0', '-1')}")# Should be ['c', 'b', 'a', 'd', 'e']
    print(f"LRANGE mylist -2 -1: {store.command_lrange('mylist', '-2', '-1')}")# Should be ['d', 'e']
    print(f"LRANGE mylist 5 10: {store.command_lrange('mylist', '5', '10')}")# Should be []
    print(f"LRANGE non_existent_list 0 -1: {store.command_lrange('non_existent_list', '0', '-1')}") # Should be []

    print("\n--- Testing Type Errors ---")
    print(f"SET mystring hello: {store.command_set('mystring', 'hello')}")
    print(f"LPUSH mystring world: {store.command_lpush('mystring', 'world')}") # Should fail (WRONGTYPE)
    print(f"GET mylist: {store.command_get('mylist')}") # Should fail (WRONGTYPE)
    print(f"DEL mystring mylist: {store.command_del('mystring', 'mylist')}")

    print("\n--- Testing Expiry with Lists ---")
    print(f"LPUSH temp_list x y: {store.command_lpush('temp_list', 'x', 'y')}")
    print(f"EXPIRE temp_list 2: {store.command_expire('temp_list', '2')}")
    print(f"LRANGE temp_list 0 -1: {store.command_lrange('temp_list', '0', '-1')}")
    print("Waiting 3 seconds...")
    time.sleep(3)
    print(f"LRANGE temp_list 0 -1 (should be empty): {store.command_lrange('temp_list', '0', '-1')}")
