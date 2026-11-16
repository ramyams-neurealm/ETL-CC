{
  "format": "AB_INITIO_GRAPH_JSON",
  "format_version": "1.0",
  "file_extension": ".mp",
  "project": "customer_analytics",
  "graphs": [
    {
      "name": "g_copy_active_customers",
      "version": "sample-v1",
      "description": "Copy customer identifiers and contact details from the active customer source to the active customer target.",
      "unsupported_constructs": [],
      "parameters": [],
      "datasets": {
        "sources": [
          {
            "name": "SRC_ACTIVE_CUSTOMERS",
            "connection_name": "sample_customer_source",
            "fields": [
              {"name": "CUSTOMER_ID", "data_type": "INTEGER", "nullable": false},
              {"name": "EMAIL", "data_type": "STRING", "nullable": true},
              {"name": "CUSTOMER_STATUS", "data_type": "STRING", "nullable": false}
            ]
          }
        ],
        "targets": [
          {
            "name": "TGT_ACTIVE_CUSTOMERS",
            "connection_name": "sample_customer_target",
            "fields": [
              {"name": "CUSTOMER_ID", "data_type": "INTEGER", "nullable": false},
              {"name": "EMAIL", "data_type": "STRING", "nullable": true},
              {"name": "CUSTOMER_STATUS", "data_type": "STRING", "nullable": false}
            ]
          }
        ]
      },
      "components": [
        {
          "name": "REFORMAT_ACTIVE_CUSTOMERS",
          "type": "REFORMAT",
          "ports": [
            {"name": "CUSTOMER_ID", "direction": "OUTPUT", "expression": "CUSTOMER_ID"},
            {"name": "EMAIL", "direction": "OUTPUT", "expression": "EMAIL"},
            {"name": "CUSTOMER_STATUS", "direction": "OUTPUT", "expression": "CUSTOMER_STATUS"}
          ]
        }
      ],
      "edges": [
        {"from_instance": "SRC_ACTIVE_CUSTOMERS", "to_instance": "REFORMAT_ACTIVE_CUSTOMERS"},
        {"from_instance": "REFORMAT_ACTIVE_CUSTOMERS", "to_instance": "TGT_ACTIVE_CUSTOMERS"}
      ],
      "field_links": [
        {"from_instance": "SRC_ACTIVE_CUSTOMERS", "from_field": "CUSTOMER_ID", "to_instance": "REFORMAT_ACTIVE_CUSTOMERS", "to_field": "CUSTOMER_ID"},
        {"from_instance": "SRC_ACTIVE_CUSTOMERS", "from_field": "EMAIL", "to_instance": "REFORMAT_ACTIVE_CUSTOMERS", "to_field": "EMAIL"},
        {"from_instance": "SRC_ACTIVE_CUSTOMERS", "from_field": "CUSTOMER_STATUS", "to_instance": "REFORMAT_ACTIVE_CUSTOMERS", "to_field": "CUSTOMER_STATUS"},
        {"from_instance": "REFORMAT_ACTIVE_CUSTOMERS", "from_field": "CUSTOMER_ID", "to_instance": "TGT_ACTIVE_CUSTOMERS", "to_field": "CUSTOMER_ID"},
        {"from_instance": "REFORMAT_ACTIVE_CUSTOMERS", "from_field": "EMAIL", "to_instance": "TGT_ACTIVE_CUSTOMERS", "to_field": "EMAIL"},
        {"from_instance": "REFORMAT_ACTIVE_CUSTOMERS", "from_field": "CUSTOMER_STATUS", "to_instance": "TGT_ACTIVE_CUSTOMERS", "to_field": "CUSTOMER_STATUS"}
      ]
    }
  ]
}
