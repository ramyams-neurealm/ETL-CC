{
  "format": "AB_INITIO_GRAPH_JSON",
  "format_version": "1.0",
  "file_extension": ".mp",
  "project": "inventory_operations",
  "graphs": [
    {
      "name": "g_build_inventory_reorder_queue",
      "version": "sample-v1",
      "description": "Select inventory items below their reorder level and calculate the quantity needed to replenish each item.",
      "unsupported_constructs": [],
      "parameters": [],
      "datasets": {
        "sources": [
          {
            "name": "SRC_INVENTORY",
            "connection_name": "sample_inventory_source",
            "fields": [
              {"name": "ITEM_ID", "data_type": "STRING", "nullable": false},
              {"name": "WAREHOUSE_ID", "data_type": "STRING", "nullable": false},
              {"name": "ON_HAND_QTY", "data_type": "INTEGER", "nullable": false},
              {"name": "REORDER_LEVEL", "data_type": "INTEGER", "nullable": false}
            ]
          }
        ],
        "targets": [
          {
            "name": "TGT_REORDER_QUEUE",
            "connection_name": "sample_reorder_target",
            "fields": [
              {"name": "ITEM_ID", "data_type": "STRING", "nullable": false},
              {"name": "WAREHOUSE_ID", "data_type": "STRING", "nullable": false},
              {"name": "ON_HAND_QTY", "data_type": "INTEGER", "nullable": false},
              {"name": "REORDER_QTY", "data_type": "INTEGER", "nullable": false}
            ]
          }
        ]
      },
      "components": [
        {
          "name": "FILTER_BELOW_REORDER_LEVEL",
          "type": "FILTER",
          "properties": {"condition": "ON_HAND_QTY < REORDER_LEVEL"}
        },
        {
          "name": "REFORMAT_REORDER_QUEUE",
          "type": "REFORMAT",
          "ports": [
            {"name": "ITEM_ID", "direction": "OUTPUT", "expression": "ITEM_ID"},
            {"name": "WAREHOUSE_ID", "direction": "OUTPUT", "expression": "WAREHOUSE_ID"},
            {"name": "ON_HAND_QTY", "direction": "OUTPUT", "expression": "ON_HAND_QTY"},
            {"name": "REORDER_QTY", "direction": "OUTPUT", "expression": "REORDER_LEVEL - ON_HAND_QTY"}
          ]
        }
      ],
      "edges": [
        {"from_instance": "SRC_INVENTORY", "to_instance": "FILTER_BELOW_REORDER_LEVEL"},
        {"from_instance": "FILTER_BELOW_REORDER_LEVEL", "to_instance": "REFORMAT_REORDER_QUEUE"},
        {"from_instance": "REFORMAT_REORDER_QUEUE", "to_instance": "TGT_REORDER_QUEUE"}
      ],
      "field_links": [
        {"from_instance": "SRC_INVENTORY", "from_field": "ITEM_ID", "to_instance": "FILTER_BELOW_REORDER_LEVEL", "to_field": "ITEM_ID"},
        {"from_instance": "SRC_INVENTORY", "from_field": "WAREHOUSE_ID", "to_instance": "FILTER_BELOW_REORDER_LEVEL", "to_field": "WAREHOUSE_ID"},
        {"from_instance": "SRC_INVENTORY", "from_field": "ON_HAND_QTY", "to_instance": "FILTER_BELOW_REORDER_LEVEL", "to_field": "ON_HAND_QTY"},
        {"from_instance": "SRC_INVENTORY", "from_field": "REORDER_LEVEL", "to_instance": "FILTER_BELOW_REORDER_LEVEL", "to_field": "REORDER_LEVEL"},
        {"from_instance": "FILTER_BELOW_REORDER_LEVEL", "from_field": "ITEM_ID", "to_instance": "REFORMAT_REORDER_QUEUE", "to_field": "ITEM_ID"},
        {"from_instance": "FILTER_BELOW_REORDER_LEVEL", "from_field": "WAREHOUSE_ID", "to_instance": "REFORMAT_REORDER_QUEUE", "to_field": "WAREHOUSE_ID"},
        {"from_instance": "FILTER_BELOW_REORDER_LEVEL", "from_field": "ON_HAND_QTY", "to_instance": "REFORMAT_REORDER_QUEUE", "to_field": "ON_HAND_QTY"},
        {"from_instance": "FILTER_BELOW_REORDER_LEVEL", "from_field": "REORDER_LEVEL", "to_instance": "REFORMAT_REORDER_QUEUE", "to_field": "REORDER_LEVEL"},
        {"from_instance": "REFORMAT_REORDER_QUEUE", "from_field": "ITEM_ID", "to_instance": "TGT_REORDER_QUEUE", "to_field": "ITEM_ID"},
        {"from_instance": "REFORMAT_REORDER_QUEUE", "from_field": "WAREHOUSE_ID", "to_instance": "TGT_REORDER_QUEUE", "to_field": "WAREHOUSE_ID"},
        {"from_instance": "REFORMAT_REORDER_QUEUE", "from_field": "ON_HAND_QTY", "to_instance": "TGT_REORDER_QUEUE", "to_field": "ON_HAND_QTY"},
        {"from_instance": "REFORMAT_REORDER_QUEUE", "from_field": "REORDER_QTY", "to_instance": "TGT_REORDER_QUEUE", "to_field": "REORDER_QTY"}
      ]
    }
  ]
}
