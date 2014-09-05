authentication = {
    "type": "object",
    "properties": {
        "access": {
            "type": "object",
            "properties": {
                "token": {
                    "type": "object",
                    "properties": {
                        "id": {
                            "type": "string"
                        },
                    },
                    "required": ["id"],
                },
                "serviceCatalog": {
                    "type": "array",
                },
            },
            "required": ["token", "serviceCatalog", ],
        },
    },
    "required": ["access", ],
}
