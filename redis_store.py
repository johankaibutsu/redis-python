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

    # --- Hash Commands ---
    def command_hset(self, key, field, value):
        """Sets the value of a field in a hash stored at key."""
        print(f"Executing: HSET {key} {field} {value}")

        if self._check_expiry(key):
            # Key expired, create a new hash
            self._data[key] = {field: value}
            if key in self._expirations: del self._expirations[key]
            print(f"Created new hash for key '{key}' after expiry.")
            return 1 # 1 field added

        current_value = self._data.get(key)
        if current_value is None:
            self._data[key] = {field: value}
            if key in self._expirations: del self._expirations[key]
            print(f"Created new hash for key '{key}'.")
            return 1
        elif isinstance(current_value, dict):
            is_new_field = field not in current_value
            current_value[field] = value
            print(f"Set field '{field}' in hash '{key}'. New field: {is_new_field}")
            return 1 if is_new_field else 0
        else:
            error_msg = "WRONGTYPE Operation against a key holding the wrong kind of value"
            print(f"Error for key '{key}': {error_msg}")
            return f"ERROR: {error_msg}"

    def command_hget(self, key, field):
        """Gets the value of a field in a hash stored at key."""
        print(f"Executing: HGET {key} {field}")

        value, error = self._get_value_or_error(key, expected_type=dict)
        if error:
            return f"ERROR: {error}"
        if value is None:
            print(f"Hash '{key}' not found or expired.")
            return None

        field_value = value.get(field, None)
        print(f"Retrieved field '{field}' from hash '{key}': {field_value}")
        return field_value

    def command_hdel(self, key, *fields):
        """Deletes one or more fields from a hash stored at key."""
        print(f"Executing: HDEL {key} {' '.join(fields)}")
        if not fields:
             print("Error: HDEL requires at least one field.")
             return "ERROR: wrong number of arguments for 'hdel' command"

        value, error = self._get_value_or_error(key, expected_type=dict)
        if error:
            return f"ERROR: {error}"
        if value is None:
            print(f"Hash '{key}' not found or expired. Cannot delete fields.")
            return 0

        deleted_count = 0
        for field in fields:
            if field in value:
                del value[field]
                deleted_count += 1
                print(f"Deleted field '{field}' from hash '{key}'.")
            else:
                 print(f"Field '{field}' not found in hash '{key}'.")

        if not value:
            self._delete_key_internal(key)
            print(f"Hash '{key}' became empty and was deleted.")

        print(f"Deleted {deleted_count} fields from hash '{key}'.")
        return deleted_count

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
    # Set with expiration (e.g., 2000ms = 2 seconds)
    print(f"SET temp_key temp_value EX 2000: {store.command_set('temp_key', 'temp_value', expire_ms='2000')}")
    print(f"GET temp_key (should exist): {store.command_get('temp_key')}")
    print(f"TTL temp_key (should be > 0): {store.command_ttl('temp_key')}")
    print("Waiting for 3 seconds...")
    time.sleep(3)
    print(f"GET temp_key (should be None): {store.command_get('temp_key')}")
    print(f"TTL temp_key (should be -2): {store.command_ttl('temp_key')}") # Should be -2 (non-existent or expired)

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
    print(f"DEL short_lived: {store.command_del('short_lived')}") # Should return 0 as it expired

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

    print("\n--- Testing Hash Commands ---")
    print(f"HSET user:1 name Johan: {store.command_hset('user:1', 'name', 'Johan')}") # New hash, new field = 1
    print(f"HSET user:1 age 23: {store.command_hset('user:1', 'age', '23')}")     # Existing hash, new field = 1
    print(f"HSET user:1 name Jojo: {store.command_hset('user:1', 'name', 'Jojo')}")# Existing hash, existing field = 0
    print(f"HGET user:1 name: {store.command_hget('user:1', 'name')}")           # Should be Jojo
    print(f"HGET user:1 age: {store.command_hget('user:1', 'age')}")             # Should be 23
    print(f"HGET user:1 city: {store.command_hget('user:1', 'city')}")           # Field doesn't exist = None
    print(f"HGET non_existent_user name: {store.command_hget('non_existent_user', 'name')}") # Key doesn't exist = None
    print(f"HDEL user:1 age city: {store.command_hdel('user:1', 'age', 'city')}") # Delete existing 'age' and non-existing 'city' = 1
    print(f"HGET user:1 age: {store.command_hget('user:1', 'age')}")             # Should be None
    print(f"HGET user:1 name: {store.command_hget('user:1', 'name')}")           # Should still be Jojo
    print(f"HDEL user:1 name: {store.command_hdel('user:1', 'name')}")           # Delete last field = 1
    print(f"HGET user:1 name: {store.command_hget('user:1', 'name')}")           # Should be None (field gone)
    # Key user:1 still exists but is an empty hash now. Let's check TTL (should be -1 unless EXPIREd)
    print(f"TTL user:1: {store.command_ttl('user:1')}")
    print(f"DEL user:1: {store.command_del('user:1')}") # Clean up the empty hash key

    print("\n--- Testing Type Errors with Hashes ---")
    print(f"SET mystring again: {store.command_set('mystring', 'again')}")
    print(f"HSET mystring field value: {store.command_hset('mystring', 'field', 'value')}") # Should fail (WRONGTYPE)
    print(f"LPUSH mylist again: {store.command_lpush('mylist', 'again')}")
    print(f"HGET mylist field: {store.command_hget('mylist', 'field')}") # Should fail (WRONGTYPE)
    print(f"DEL mystring mylist: {store.command_del('mystring', 'mylist')}")
