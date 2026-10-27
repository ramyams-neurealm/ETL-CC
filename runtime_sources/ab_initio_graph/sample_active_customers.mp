{
  "format": "AB_INITIO_GRAPH_JSON",
  "format_version": "1.0",
  "file_extension": ".mp",
  "project": "customer_analytics",
  "graphs": [
    {
      "name": "g_export_active_customers",
      "version": "sample-v1",
      "description": "Select active customers and copy their customer ID, email, and status to the active customer export.",
      "unsupported_constructs": [],
      "parameters": [],
      "datasets": {
        "sources": [
          {
            "name": "SRC_CUSTOMERS",
            "connection_name": "sample_customer_source",
            "fields": [
              {"name": "CUSTOMER_ID", "data_type": "INTEGER", "nullable": false},
              {"name": "EMAIL", "data_type": "STRING", "nullable": true},
              {"name": "STATUS", "data_type": "STRING", "nullable": false}
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
          "name": "FILTER_ACTIVE_CUSTOMERS",
          "type": "FILTER",
          "properties": {"condition": "STATUS == 'ACTIVE'"}
        },
        {
          "name": "REFORMAT_CUSTOMER_EXPORT",
          "type": "REFORMAT",
          "ports": [
            {"name": "CUSTOMER_ID", "direction": "OUTPUT", "expression": "CUSTOMER_ID"},
            {"name": "EMAIL", "direction": "OUTPUT", "expression": "EMAIL"},
            {"name": "CUSTOMER_STATUS", "direction": "OUTPUT", "expression": "STATUS"}
          ]
        }
      ],
      "edges": [
        {"from_instance": "SRC_CUSTOMERS", "to_instance": "FILTER_ACTIVE_CUSTOMERS"},
        {"from_instance": "FILTER_ACTIVE_CUSTOMERS", "to_instance": "REFORMAT_CUSTOMER_EXPORT"},
        {"from_instance": "REFORMAT_CUSTOMER_EXPORT", "to_instance": "TGT_ACTIVE_CUSTOMERS"}
      ],
      "field_links": [
        {"from_instance": "SRC_CUSTOMERS", "from_field": "CUSTOMER_ID", "to_instance": "FILTER_ACTIVE_CUSTOMERS", "to_field": "CUSTOMER_ID"},
        {"from_instance": "SRC_CUSTOMERS", "from_field": "EMAIL", "to_instance": "FILTER_ACTIVE_CUSTOMERS", "to_field": "EMAIL"},
        {"from_instance": "SRC_CUSTOMERS", "from_field": "STATUS", "to_instance": "FILTER_ACTIVE_CUSTOMERS", "to_field": "STATUS"},
        {"from_instance": "FILTER_ACTIVE_CUSTOMERS", "from_field": "CUSTOMER_ID", "to_instance": "REFORMAT_CUSTOMER_EXPORT", "to_field": "CUSTOMER_ID"},
        {"from_instance": "FILTER_ACTIVE_CUSTOMERS", "from_field": "EMAIL", "to_instance": "REFORMAT_CUSTOMER_EXPORT", "to_field": "EMAIL"},
        {"from_instance": "FILTER_ACTIVE_CUSTOMERS", "from_field": "STATUS", "to_instance": "REFORMAT_CUSTOMER_EXPORT", "to_field": "STATUS"},
        {"from_instance": "REFORMAT_CUSTOMER_EXPORT", "from_field": "CUSTOMER_ID", "to_instance": "TGT_ACTIVE_CUSTOMERS", "to_field": "CUSTOMER_ID"},
        {"from_instance": "REFORMAT_CUSTOMER_EXPORT", "from_field": "EMAIL", "to_instance": "TGT_ACTIVE_CUSTOMERS", "to_field": "EMAIL"},
        {"from_instance": "REFORMAT_CUSTOMER_EXPORT", "from_field": "CUSTOMER_STATUS", "to_instance": "TGT_ACTIVE_CUSTOMERS", "to_field": "CUSTOMER_STATUS"}
      ]
    }
  ]
}
