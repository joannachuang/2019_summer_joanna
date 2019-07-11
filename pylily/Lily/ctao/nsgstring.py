

def alnum(your_string):
    import re
    your_string = re.sub(r':','_ddsk_', your_string)
    your_string = re.sub(r'/','_ppth_', your_string)
    your_string = re.sub(r'\W+', '_', your_string)
    return your_string

def alnum_uuid():
    """Returns a random string of length string_length."""
    import uuid

# Convert UUID format to a Python string.
    random = str(uuid.uuid4())

# Make all characters uppercase.
    random = random.upper()

# Remove the UUID '-'.
    random = random.replace("-","_")

# Return the random string.
    return random
