vault_statistics = {
    "type": "object",
    "properties": {
        "storage": {
            "type": "object",
            "properties": {
                "block-count": {
                    "type": "integer",
                    "minimum": 0,
                },
                "total-size": {
                    "type": "integer",
                    "minimum": 0,
                },
                "internal": {
                    "type": "object",
                    "properties": {
                        "last-modification-time": {
                            "type": "string",
                            "pattern": r'^\d{1,}\.\d{1,}$',
                        },
                    },
                    "additionalProperties": False,
                },
            },
            "required": ["block-count", "total-size", "internal", ],
            "additionalProperties": False,
        },
        "metadata": {
            "type": "object",
            "properties": {
                "files": {
                    "type": "object",
                    "properties": {
                        "count": {
                            "type": "integer",
                            "minimum": 0,
                        },
                    },
                    "required": ["count", ],
                    "additionalProperties": False,
                },
                "internal": {
                    "type": "object",
                },
                "blocks": {
                    "type": "object",
                    "properties": {
                        "count": {
                            "type": "integer",
                            "minimum": 0,
                        },
                    },
                    "required": ["count", ],
                    "additionalProperties": False,
                },
            },
            "required": ["files", "internal", "blocks", ],
            "additionalProperties": False,
        },
    },
    "required": ["storage", "metadata", ],
    "additionalProperties": False,
}

error = {
    "type": "object",
    "properties": {
        "title": {
            "type": "string",
        },
        "description": {
            "type": "string",
        },
    },
    "required": ["title", "description", ],
    "additionalProperties": False,
}

vault_list = {
    "type": "object",
    "patternProperties": {
        r"^[a-zA-Z0-9\-_]{1,128}$": {
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                },
            },
            "required": ["url", ],
            "additionalProperties": False,
        },
    },
    "additionalProperties": False,
}

block_list = {
    "type": "array",
    "items": {
        "type": "string",
        "maxLength": 40,
        "pattern": r"^[a-z0-9]{40}$",
    },
    "minItems": 0,
    "uniqueItems": True,
}

file_list = {
    "type": "array",
    "items": {
        "type": "string",
        "maxLength": 40,
        "pattern": r"^[a-z0-9]{8}\-[a-z0-9]{4}\-[1-5][a-z0-9]{3}\-"
                   "[ab89][a-z0-9]{3}\-[a-z0-9]{12}$",
    },
    "minItems": 0,
    "uniqueItems": True,
}

block_list_of_file = {
    "type": "array",
    "items": {
        "type": "array",
        "items": {
            "oneOf": [
                {
                    "type": "string",
                    "maxLength": 40,
                    "pattern": r"^[a-z0-9]{40}$",
                },
                {
                    "type": "integer",
                    "minimum": 0,
                },
            ],
        },
        "uniqueItems": True,
        "minItems": 2,
        "maxItems": 2,
    },
    "minItems": 0,
    "uniqueItems": True,
}
