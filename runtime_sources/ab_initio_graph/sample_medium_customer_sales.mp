{
  "format": "AB_INITIO_GRAPH_JSON",
  "format_version": "1.0",
  "file_extension": ".mp",
  "project": "customer_analytics",
  "graphs": [
    {
      "name": "g_build_customer_category_sales",
      "version": "synthetic-v1",
      "description": "Filter completed orders, join order lines, look up product categories, calculate line sales, and aggregate sales and line counts by customer and category.",
      "unsupported_constructs": [],
      "parameters": [],
      "datasets": {
        "sources": [
          {
            "name": "SRC_ORDERS",
            "connection_name": "sample_orders_source",
            "fields": [
              {"name": "ORDER_ID", "data_type": "STRING", "nullable": false},
              {"name": "CUSTOMER_ID", "data_type": "INTEGER", "nullable": false},
              {"name": "ORDER_STATUS", "data_type": "STRING", "nullable": false}
            ]
          },
          {
            "name": "SRC_ORDER_LINES",
            "connection_name": "sample_order_lines_source",
            "fields": [
              {"name": "ORDER_ID", "data_type": "STRING", "nullable": false},
              {"name": "PRODUCT_ID", "data_type": "STRING", "nullable": false},
              {"name": "QUANTITY", "data_type": "INTEGER", "nullable": false},
              {"name": "UNIT_PRICE", "data_type": "DECIMAL", "precision": 12, "scale": 2, "nullable": false}
            ]
          },
          {
            "name": "SRC_PRODUCTS",
            "connection_name": "sample_products_source",
            "fields": [
              {"name": "PRODUCT_ID", "data_type": "STRING", "nullable": false},
              {"name": "CATEGORY", "data_type": "STRING", "nullable": false}
            ]
          }
        ],
        "targets": [
          {
            "name": "TGT_CUSTOMER_CATEGORY_SALES",
            "connection_name": "sample_customer_sales_target",
            "fields": [
              {"name": "CUSTOMER_ID", "data_type": "INTEGER", "nullable": false},
              {"name": "CATEGORY", "data_type": "STRING", "nullable": false},
              {"name": "TOTAL_SALES", "data_type": "DECIMAL", "precision": 14, "scale": 2, "nullable": false},
              {"name": "LINE_COUNT", "data_type": "INTEGER", "nullable": false}
            ]
          }
        ]
      },
      "components": [
        {
          "name": "FILTER_COMPLETED_ORDERS",
          "type": "FILTER",
          "properties": {"condition": "ORDER_STATUS == 'COMPLETE'"}
        },
        {
          "name": "JOIN_ORDERS_TO_LINES",
          "type": "JOINER",
          "properties": {
            "join_type": "INNER",
            "condition": "SRC_ORDERS.ORDER_ID == SRC_ORDER_LINES.ORDER_ID"
          }
        },
        {
          "name": "LOOKUP_PRODUCT_CATEGORY",
          "type": "LOOKUP",
          "properties": {
            "lookup_dataset": "SRC_PRODUCTS",
            "lookup_condition": "SRC_ORDER_LINES.PRODUCT_ID == SRC_PRODUCTS.PRODUCT_ID",
            "multiple_match_policy": "ERROR"
          }
        },
        {
          "name": "REFORMAT_LINE_SALES",
          "type": "REFORMAT",
          "ports": [
            {"name": "CUSTOMER_ID", "direction": "OUTPUT", "expression": "CUSTOMER_ID"},
            {"name": "CATEGORY", "direction": "OUTPUT", "expression": "CATEGORY"},
            {"name": "LINE_AMOUNT", "direction": "OUTPUT", "expression": "QUANTITY * UNIT_PRICE"}
          ]
        },
        {
          "name": "AGGREGATE_CUSTOMER_CATEGORY_SALES",
          "type": "AGGREGATOR",
          "properties": {
            "group_by": ["CUSTOMER_ID", "CATEGORY"],
            "aggregations": {
              "TOTAL_SALES": "sum(LINE_AMOUNT)",
              "LINE_COUNT": "count(LINE_AMOUNT)"
            }
          }
        }
      ],
      "edges": [
        {"from_instance": "SRC_ORDERS", "to_instance": "FILTER_COMPLETED_ORDERS"},
        {"from_instance": "FILTER_COMPLETED_ORDERS", "to_instance": "JOIN_ORDERS_TO_LINES"},
        {"from_instance": "SRC_ORDER_LINES", "to_instance": "JOIN_ORDERS_TO_LINES"},
        {"from_instance": "JOIN_ORDERS_TO_LINES", "to_instance": "LOOKUP_PRODUCT_CATEGORY"},
        {"from_instance": "SRC_PRODUCTS", "to_instance": "LOOKUP_PRODUCT_CATEGORY"},
        {"from_instance": "LOOKUP_PRODUCT_CATEGORY", "to_instance": "REFORMAT_LINE_SALES"},
        {"from_instance": "REFORMAT_LINE_SALES", "to_instance": "AGGREGATE_CUSTOMER_CATEGORY_SALES"},
        {"from_instance": "AGGREGATE_CUSTOMER_CATEGORY_SALES", "to_instance": "TGT_CUSTOMER_CATEGORY_SALES"}
      ],
      "field_links": [
        {"from_instance": "SRC_ORDERS", "from_field": "ORDER_ID", "to_instance": "FILTER_COMPLETED_ORDERS", "to_field": "ORDER_ID"},
        {"from_instance": "SRC_ORDERS", "from_field": "CUSTOMER_ID", "to_instance": "FILTER_COMPLETED_ORDERS", "to_field": "CUSTOMER_ID"},
        {"from_instance": "SRC_ORDERS", "from_field": "ORDER_STATUS", "to_instance": "FILTER_COMPLETED_ORDERS", "to_field": "ORDER_STATUS"},

        {"from_instance": "FILTER_COMPLETED_ORDERS", "from_field": "ORDER_ID", "to_instance": "JOIN_ORDERS_TO_LINES", "to_field": "ORDER_ID"},
        {"from_instance": "FILTER_COMPLETED_ORDERS", "from_field": "CUSTOMER_ID", "to_instance": "JOIN_ORDERS_TO_LINES", "to_field": "CUSTOMER_ID"},
        {"from_instance": "SRC_ORDER_LINES", "from_field": "ORDER_ID", "to_instance": "JOIN_ORDERS_TO_LINES", "to_field": "ORDER_ID"},
        {"from_instance": "SRC_ORDER_LINES", "from_field": "PRODUCT_ID", "to_instance": "JOIN_ORDERS_TO_LINES", "to_field": "PRODUCT_ID"},
        {"from_instance": "SRC_ORDER_LINES", "from_field": "QUANTITY", "to_instance": "JOIN_ORDERS_TO_LINES", "to_field": "QUANTITY"},
        {"from_instance": "SRC_ORDER_LINES", "from_field": "UNIT_PRICE", "to_instance": "JOIN_ORDERS_TO_LINES", "to_field": "UNIT_PRICE"},

        {"from_instance": "JOIN_ORDERS_TO_LINES", "from_field": "CUSTOMER_ID", "to_instance": "LOOKUP_PRODUCT_CATEGORY", "to_field": "CUSTOMER_ID"},
        {"from_instance": "JOIN_ORDERS_TO_LINES", "from_field": "PRODUCT_ID", "to_instance": "LOOKUP_PRODUCT_CATEGORY", "to_field": "PRODUCT_ID"},
        {"from_instance": "JOIN_ORDERS_TO_LINES", "from_field": "QUANTITY", "to_instance": "LOOKUP_PRODUCT_CATEGORY", "to_field": "QUANTITY"},
        {"from_instance": "JOIN_ORDERS_TO_LINES", "from_field": "UNIT_PRICE", "to_instance": "LOOKUP_PRODUCT_CATEGORY", "to_field": "UNIT_PRICE"},
        {"from_instance": "SRC_PRODUCTS", "from_field": "PRODUCT_ID", "to_instance": "LOOKUP_PRODUCT_CATEGORY", "to_field": "PRODUCT_ID"},
        {"from_instance": "SRC_PRODUCTS", "from_field": "CATEGORY", "to_instance": "LOOKUP_PRODUCT_CATEGORY", "to_field": "CATEGORY"},

        {"from_instance": "LOOKUP_PRODUCT_CATEGORY", "from_field": "CUSTOMER_ID", "to_instance": "REFORMAT_LINE_SALES", "to_field": "CUSTOMER_ID"},
        {"from_instance": "LOOKUP_PRODUCT_CATEGORY", "from_field": "CATEGORY", "to_instance": "REFORMAT_LINE_SALES", "to_field": "CATEGORY"},
        {"from_instance": "LOOKUP_PRODUCT_CATEGORY", "from_field": "QUANTITY", "to_instance": "REFORMAT_LINE_SALES", "to_field": "QUANTITY"},
        {"from_instance": "LOOKUP_PRODUCT_CATEGORY", "from_field": "UNIT_PRICE", "to_instance": "REFORMAT_LINE_SALES", "to_field": "UNIT_PRICE"},

        {"from_instance": "REFORMAT_LINE_SALES", "from_field": "CUSTOMER_ID", "to_instance": "AGGREGATE_CUSTOMER_CATEGORY_SALES", "to_field": "CUSTOMER_ID"},
        {"from_instance": "REFORMAT_LINE_SALES", "from_field": "CATEGORY", "to_instance": "AGGREGATE_CUSTOMER_CATEGORY_SALES", "to_field": "CATEGORY"},
        {"from_instance": "REFORMAT_LINE_SALES", "from_field": "LINE_AMOUNT", "to_instance": "AGGREGATE_CUSTOMER_CATEGORY_SALES", "to_field": "LINE_AMOUNT"},

        {"from_instance": "AGGREGATE_CUSTOMER_CATEGORY_SALES", "from_field": "CUSTOMER_ID", "to_instance": "TGT_CUSTOMER_CATEGORY_SALES", "to_field": "CUSTOMER_ID"},
        {"from_instance": "AGGREGATE_CUSTOMER_CATEGORY_SALES", "from_field": "CATEGORY", "to_instance": "TGT_CUSTOMER_CATEGORY_SALES", "to_field": "CATEGORY"},
        {"from_instance": "AGGREGATE_CUSTOMER_CATEGORY_SALES", "from_field": "TOTAL_SALES", "to_instance": "TGT_CUSTOMER_CATEGORY_SALES", "to_field": "TOTAL_SALES"},
        {"from_instance": "AGGREGATE_CUSTOMER_CATEGORY_SALES", "from_field": "LINE_COUNT", "to_instance": "TGT_CUSTOMER_CATEGORY_SALES", "to_field": "LINE_COUNT"}
      ]
    }
  ]
}