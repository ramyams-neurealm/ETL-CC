{
  "format": "AB_INITIO_GRAPH_JSON",
  "format_version": "1.0",
  "file_extension": ".mp",
  "project": "order_analytics",
  "graphs": [
    {
      "name": "g_build_order_profitability",
      "version": "synthetic-v1",
      "description": "Enrich order headers and lines with customer and product data, calculate order profitability, rank customer orders, and route rejected orders to a separate target.",
      "unsupported_constructs": [],
      "parameters": [
        {
          "name": "LOAD_DATE",
          "parameter_type": "DATE",
          "data_type": "DATE",
          "default_value": "CURRENT_DATE",
          "scope": "GRAPH"
        }
      ],
      "datasets": {
        "sources": [
          {
            "name": "SRC_ORDERS",
            "connection_name": "sample_order_source",
            "fields": [
              {"name": "ORDER_ID", "data_type": "STRING", "nullable": false},
              {"name": "CUSTOMER_ID", "data_type": "INTEGER", "nullable": false},
              {"name": "ORDER_TS", "data_type": "TIMESTAMP", "nullable": false},
              {"name": "ORDER_STATUS", "data_type": "STRING", "nullable": false},
              {"name": "REGION", "data_type": "STRING", "nullable": true}
            ]
          },
          {
            "name": "SRC_ORDER_LINES",
            "connection_name": "sample_order_line_source",
            "fields": [
              {"name": "ORDER_ID", "data_type": "STRING", "nullable": false},
              {"name": "PRODUCT_ID", "data_type": "STRING", "nullable": false},
              {"name": "QUANTITY", "data_type": "INTEGER", "nullable": false},
              {"name": "UNIT_PRICE", "data_type": "DECIMAL", "precision": 12, "scale": 2, "nullable": false},
              {"name": "DISCOUNT_PCT", "data_type": "DECIMAL", "precision": 5, "scale": 2, "nullable": true}
            ]
          },
          {
            "name": "SRC_CUSTOMERS",
            "connection_name": "sample_customer_source",
            "fields": [
              {"name": "CUSTOMER_ID", "data_type": "INTEGER", "nullable": false},
              {"name": "CUSTOMER_SEGMENT", "data_type": "STRING", "nullable": false},
              {"name": "COUNTRY_CODE", "data_type": "STRING", "nullable": true},
              {"name": "RISK_SCORE", "data_type": "INTEGER", "nullable": true}
            ]
          },
          {
            "name": "SRC_PRODUCTS",
            "connection_name": "sample_product_source",
            "fields": [
              {"name": "PRODUCT_ID", "data_type": "STRING", "nullable": false},
              {"name": "CATEGORY", "data_type": "STRING", "nullable": false},
              {"name": "UNIT_COST", "data_type": "DECIMAL", "precision": 12, "scale": 2, "nullable": false}
            ]
          }
        ],
        "targets": [
          {
            "name": "TGT_ORDER_PROFITABILITY",
            "connection_name": "sample_order_analytics_target",
            "fields": [
              {"name": "ORDER_ID", "data_type": "STRING", "nullable": false},
              {"name": "CUSTOMER_ID", "data_type": "INTEGER", "nullable": false},
              {"name": "CUSTOMER_SEGMENT", "data_type": "STRING", "nullable": false},
              {"name": "ORDER_AMOUNT", "data_type": "DECIMAL", "precision": 14, "scale": 2, "nullable": false},
              {"name": "GROSS_MARGIN", "data_type": "DECIMAL", "precision": 14, "scale": 2, "nullable": false},
              {"name": "CUSTOMER_ORDER_RANK", "data_type": "INTEGER", "nullable": false},
              {"name": "LOAD_DATE", "data_type": "DATE", "nullable": false}
            ]
          },
          {
            "name": "TGT_ORDER_REJECTS",
            "connection_name": "sample_order_reject_target",
            "fields": [
              {"name": "ORDER_ID", "data_type": "STRING", "nullable": false},
              {"name": "CUSTOMER_ID", "data_type": "INTEGER", "nullable": false},
              {"name": "REJECT_REASON", "data_type": "STRING", "nullable": false},
              {"name": "LOAD_DATE", "data_type": "DATE", "nullable": false}
            ]
          }
        ]
      },
      "components": [
        {
          "name": "FILTER_COMPLETED_ORDERS",
          "type": "FILTER",
          "properties": {"condition": "ORDER_STATUS == 'COMPLETE' && ORDER_ID != null"}
        },
        {
          "name": "LOOKUP_CUSTOMER_PROFILE",
          "type": "LOOKUP",
          "properties": {
            "lookup_dataset": "SRC_CUSTOMERS",
            "lookup_condition": "SRC_ORDERS.CUSTOMER_ID == SRC_CUSTOMERS.CUSTOMER_ID",
            "multiple_match_policy": "ERROR"
          }
        },
        {
          "name": "LOOKUP_PRODUCT_COST",
          "type": "LOOKUP",
          "properties": {
            "lookup_dataset": "SRC_PRODUCTS",
            "lookup_condition": "SRC_ORDER_LINES.PRODUCT_ID == SRC_PRODUCTS.PRODUCT_ID",
            "multiple_match_policy": "FIRST"
          }
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
          "name": "REFORMAT_LINE_PROFIT",
          "type": "REFORMAT",
          "ports": [
            {"name": "ORDER_ID", "direction": "OUTPUT", "expression": "ORDER_ID"},
            {"name": "CUSTOMER_ID", "direction": "OUTPUT", "expression": "CUSTOMER_ID"},
            {"name": "CUSTOMER_SEGMENT", "direction": "OUTPUT", "expression": "CUSTOMER_SEGMENT"},
            {"name": "EXTENDED_AMOUNT", "direction": "OUTPUT", "expression": "QUANTITY * UNIT_PRICE * (1 - coalesce(DISCOUNT_PCT, 0) / 100)"},
            {"name": "EXTENDED_COST", "direction": "OUTPUT", "expression": "QUANTITY * UNIT_COST"},
            {"name": "RISK_SCORE", "direction": "OUTPUT", "expression": "coalesce(RISK_SCORE, 0)"}
          ]
        },
        {
          "name": "AGGREGATE_ORDER_TOTALS",
          "type": "AGGREGATOR",
          "properties": {
            "group_by": ["ORDER_ID", "CUSTOMER_ID", "CUSTOMER_SEGMENT", "RISK_SCORE"],
            "aggregations": {
              "ORDER_AMOUNT": "sum(EXTENDED_AMOUNT)",
              "TOTAL_COST": "sum(EXTENDED_COST)"
            }
          }
        },
        {
          "name": "FILTER_MINIMUM_ORDER",
          "type": "FILTER",
          "properties": {"condition": "ORDER_AMOUNT >= 50.00"}
        },
        {
          "name": "SORT_CUSTOMER_ORDERS",
          "type": "SORTER",
          "properties": {
            "keys": [
              {"field": "CUSTOMER_ID", "direction": "ASC"},
              {"field": "ORDER_AMOUNT", "direction": "DESC"}
            ]
          }
        },
        {
          "name": "RANK_ORDERS_PER_CUSTOMER",
          "type": "RANK",
          "properties": {
            "partition_by": ["CUSTOMER_ID"],
            "order_by": [{"field": "ORDER_AMOUNT", "direction": "DESC"}],
            "output_field": "CUSTOMER_ORDER_RANK"
          }
        },
        {
          "name": "ROUTE_VALID_AND_HIGH_RISK",
          "type": "ROUTER",
          "properties": {
            "groups": {
              "VALID": "RISK_SCORE < 80",
              "HIGH_RISK": "RISK_SCORE >= 80"
            },
            "default_group": "HIGH_RISK"
          }
        },
        {
          "name": "REFORMAT_PROFITABILITY_TARGET",
          "type": "REFORMAT",
          "ports": [
            {"name": "ORDER_ID", "direction": "OUTPUT", "expression": "ORDER_ID"},
            {"name": "CUSTOMER_ID", "direction": "OUTPUT", "expression": "CUSTOMER_ID"},
            {"name": "CUSTOMER_SEGMENT", "direction": "OUTPUT", "expression": "CUSTOMER_SEGMENT"},
            {"name": "ORDER_AMOUNT", "direction": "OUTPUT", "expression": "ORDER_AMOUNT"},
            {"name": "GROSS_MARGIN", "direction": "OUTPUT", "expression": "ORDER_AMOUNT - TOTAL_COST"},
            {"name": "CUSTOMER_ORDER_RANK", "direction": "OUTPUT", "expression": "CUSTOMER_ORDER_RANK"},
            {"name": "LOAD_DATE", "direction": "OUTPUT", "expression": "$LOAD_DATE"}
          ]
        },
        {
          "name": "UPDATE_PROFITABILITY_TARGET",
          "type": "UPDATE STRATEGY",
          "properties": {
            "key_fields": ["ORDER_ID"],
            "strategy": "UPSERT"
          }
        },
        {
          "name": "REFORMAT_REJECT_TARGET",
          "type": "REFORMAT",
          "ports": [
            {"name": "ORDER_ID", "direction": "OUTPUT", "expression": "ORDER_ID"},
            {"name": "CUSTOMER_ID", "direction": "OUTPUT", "expression": "CUSTOMER_ID"},
            {"name": "REJECT_REASON", "direction": "OUTPUT", "expression": "'CUSTOMER_RISK_LIMIT_EXCEEDED'"},
            {"name": "LOAD_DATE", "direction": "OUTPUT", "expression": "$LOAD_DATE"}
          ]
        }
      ],
      "edges": [
        {"from_instance": "SRC_ORDERS", "to_instance": "FILTER_COMPLETED_ORDERS"},
        {"from_instance": "FILTER_COMPLETED_ORDERS", "to_instance": "JOIN_ORDERS_TO_LINES"},
        {"from_instance": "SRC_ORDER_LINES", "to_instance": "JOIN_ORDERS_TO_LINES"},
        {"from_instance": "JOIN_ORDERS_TO_LINES", "to_instance": "LOOKUP_PRODUCT_COST"},
        {"from_instance": "SRC_PRODUCTS", "to_instance": "LOOKUP_PRODUCT_COST"},
        {"from_instance": "LOOKUP_PRODUCT_COST", "to_instance": "LOOKUP_CUSTOMER_PROFILE"},
        {"from_instance": "SRC_CUSTOMERS", "to_instance": "LOOKUP_CUSTOMER_PROFILE"},
        {"from_instance": "LOOKUP_CUSTOMER_PROFILE", "to_instance": "REFORMAT_LINE_PROFIT"},
        {"from_instance": "REFORMAT_LINE_PROFIT", "to_instance": "AGGREGATE_ORDER_TOTALS"},
        {"from_instance": "AGGREGATE_ORDER_TOTALS", "to_instance": "FILTER_MINIMUM_ORDER"},
        {"from_instance": "FILTER_MINIMUM_ORDER", "to_instance": "SORT_CUSTOMER_ORDERS"},
        {"from_instance": "SORT_CUSTOMER_ORDERS", "to_instance": "RANK_ORDERS_PER_CUSTOMER"},
        {"from_instance": "RANK_ORDERS_PER_CUSTOMER", "to_instance": "ROUTE_VALID_AND_HIGH_RISK"},
        {"from_instance": "ROUTE_VALID_AND_HIGH_RISK", "to_instance": "REFORMAT_PROFITABILITY_TARGET"},
        {"from_instance": "REFORMAT_PROFITABILITY_TARGET", "to_instance": "UPDATE_PROFITABILITY_TARGET"},
        {"from_instance": "UPDATE_PROFITABILITY_TARGET", "to_instance": "TGT_ORDER_PROFITABILITY"},
        {"from_instance": "ROUTE_VALID_AND_HIGH_RISK", "to_instance": "REFORMAT_REJECT_TARGET"},
        {"from_instance": "REFORMAT_REJECT_TARGET", "to_instance": "TGT_ORDER_REJECTS"}
      ],
      "field_links": [
        {"from_instance": "SRC_ORDERS", "from_field": "ORDER_ID", "to_instance": "FILTER_COMPLETED_ORDERS", "to_field": "ORDER_ID"},
        {"from_instance": "SRC_ORDERS", "from_field": "ORDER_STATUS", "to_instance": "FILTER_COMPLETED_ORDERS", "to_field": "ORDER_STATUS"},
        {"from_instance": "FILTER_COMPLETED_ORDERS", "from_field": "ORDER_ID", "to_instance": "JOIN_ORDERS_TO_LINES", "to_field": "ORDER_ID"},
        {"from_instance": "FILTER_COMPLETED_ORDERS", "from_field": "CUSTOMER_ID", "to_instance": "JOIN_ORDERS_TO_LINES", "to_field": "CUSTOMER_ID"},
        {"from_instance": "SRC_ORDER_LINES", "from_field": "ORDER_ID", "to_instance": "JOIN_ORDERS_TO_LINES", "to_field": "ORDER_ID"},
        {"from_instance": "SRC_ORDER_LINES", "from_field": "PRODUCT_ID", "to_instance": "JOIN_ORDERS_TO_LINES", "to_field": "PRODUCT_ID"},
        {"from_instance": "SRC_ORDER_LINES", "from_field": "QUANTITY", "to_instance": "JOIN_ORDERS_TO_LINES", "to_field": "QUANTITY"},
        {"from_instance": "SRC_ORDER_LINES", "from_field": "UNIT_PRICE", "to_instance": "JOIN_ORDERS_TO_LINES", "to_field": "UNIT_PRICE"},
        {"from_instance": "JOIN_ORDERS_TO_LINES", "from_field": "PRODUCT_ID", "to_instance": "LOOKUP_PRODUCT_COST", "to_field": "PRODUCT_ID"},
        {"from_instance": "SRC_PRODUCTS", "from_field": "PRODUCT_ID", "to_instance": "LOOKUP_PRODUCT_COST", "to_field": "PRODUCT_ID"},
        {"from_instance": "SRC_PRODUCTS", "from_field": "UNIT_COST", "to_instance": "LOOKUP_PRODUCT_COST", "to_field": "UNIT_COST"},
        {"from_instance": "LOOKUP_PRODUCT_COST", "from_field": "CUSTOMER_ID", "to_instance": "LOOKUP_CUSTOMER_PROFILE", "to_field": "CUSTOMER_ID"},
        {"from_instance": "SRC_CUSTOMERS", "from_field": "CUSTOMER_ID", "to_instance": "LOOKUP_CUSTOMER_PROFILE", "to_field": "CUSTOMER_ID"},
        {"from_instance": "SRC_CUSTOMERS", "from_field": "CUSTOMER_SEGMENT", "to_instance": "LOOKUP_CUSTOMER_PROFILE", "to_field": "CUSTOMER_SEGMENT"},
        {"from_instance": "SRC_CUSTOMERS", "from_field": "RISK_SCORE", "to_instance": "LOOKUP_CUSTOMER_PROFILE", "to_field": "RISK_SCORE"},
        {"from_instance": "LOOKUP_CUSTOMER_PROFILE", "from_field": "ORDER_ID", "to_instance": "REFORMAT_LINE_PROFIT", "to_field": "ORDER_ID"},
        {"from_instance": "LOOKUP_CUSTOMER_PROFILE", "from_field": "CUSTOMER_ID", "to_instance": "REFORMAT_LINE_PROFIT", "to_field": "CUSTOMER_ID"},
        {"from_instance": "LOOKUP_CUSTOMER_PROFILE", "from_field": "CUSTOMER_SEGMENT", "to_instance": "REFORMAT_LINE_PROFIT", "to_field": "CUSTOMER_SEGMENT"},
        {"from_instance": "LOOKUP_CUSTOMER_PROFILE", "from_field": "QUANTITY", "to_instance": "REFORMAT_LINE_PROFIT", "to_field": "QUANTITY"},
        {"from_instance": "LOOKUP_CUSTOMER_PROFILE", "from_field": "UNIT_PRICE", "to_instance": "REFORMAT_LINE_PROFIT", "to_field": "UNIT_PRICE"},
        {"from_instance": "LOOKUP_CUSTOMER_PROFILE", "from_field": "UNIT_COST", "to_instance": "REFORMAT_LINE_PROFIT", "to_field": "UNIT_COST"},
        {"from_instance": "REFORMAT_LINE_PROFIT", "from_field": "ORDER_ID", "to_instance": "AGGREGATE_ORDER_TOTALS", "to_field": "ORDER_ID"},
        {"from_instance": "REFORMAT_LINE_PROFIT", "from_field": "CUSTOMER_ID", "to_instance": "AGGREGATE_ORDER_TOTALS", "to_field": "CUSTOMER_ID"},
        {"from_instance": "REFORMAT_LINE_PROFIT", "from_field": "CUSTOMER_SEGMENT", "to_instance": "AGGREGATE_ORDER_TOTALS", "to_field": "CUSTOMER_SEGMENT"},
        {"from_instance": "REFORMAT_LINE_PROFIT", "from_field": "EXTENDED_AMOUNT", "to_instance": "AGGREGATE_ORDER_TOTALS", "to_field": "EXTENDED_AMOUNT"},
        {"from_instance": "REFORMAT_LINE_PROFIT", "from_field": "EXTENDED_COST", "to_instance": "AGGREGATE_ORDER_TOTALS", "to_field": "EXTENDED_COST"},
        {"from_instance": "REFORMAT_LINE_PROFIT", "from_field": "RISK_SCORE", "to_instance": "AGGREGATE_ORDER_TOTALS", "to_field": "RISK_SCORE"},
        {"from_instance": "AGGREGATE_ORDER_TOTALS", "from_field": "ORDER_ID", "to_instance": "FILTER_MINIMUM_ORDER", "to_field": "ORDER_ID"},
        {"from_instance": "AGGREGATE_ORDER_TOTALS", "from_field": "ORDER_AMOUNT", "to_instance": "FILTER_MINIMUM_ORDER", "to_field": "ORDER_AMOUNT"},
        {"from_instance": "FILTER_MINIMUM_ORDER", "from_field": "CUSTOMER_ID", "to_instance": "SORT_CUSTOMER_ORDERS", "to_field": "CUSTOMER_ID"},
        {"from_instance": "FILTER_MINIMUM_ORDER", "from_field": "ORDER_AMOUNT", "to_instance": "SORT_CUSTOMER_ORDERS", "to_field": "ORDER_AMOUNT"},
        {"from_instance": "SORT_CUSTOMER_ORDERS", "from_field": "CUSTOMER_ID", "to_instance": "RANK_ORDERS_PER_CUSTOMER", "to_field": "CUSTOMER_ID"},
        {"from_instance": "SORT_CUSTOMER_ORDERS", "from_field": "ORDER_AMOUNT", "to_instance": "RANK_ORDERS_PER_CUSTOMER", "to_field": "ORDER_AMOUNT"},
        {"from_instance": "RANK_ORDERS_PER_CUSTOMER", "from_field": "ORDER_ID", "to_instance": "ROUTE_VALID_AND_HIGH_RISK", "to_field": "ORDER_ID"},
        {"from_instance": "RANK_ORDERS_PER_CUSTOMER", "from_field": "CUSTOMER_ID", "to_instance": "ROUTE_VALID_AND_HIGH_RISK", "to_field": "CUSTOMER_ID"},
        {"from_instance": "RANK_ORDERS_PER_CUSTOMER", "from_field": "CUSTOMER_SEGMENT", "to_instance": "ROUTE_VALID_AND_HIGH_RISK", "to_field": "CUSTOMER_SEGMENT"},
        {"from_instance": "RANK_ORDERS_PER_CUSTOMER", "from_field": "ORDER_AMOUNT", "to_instance": "ROUTE_VALID_AND_HIGH_RISK", "to_field": "ORDER_AMOUNT"},
        {"from_instance": "RANK_ORDERS_PER_CUSTOMER", "from_field": "TOTAL_COST", "to_instance": "ROUTE_VALID_AND_HIGH_RISK", "to_field": "TOTAL_COST"},
        {"from_instance": "RANK_ORDERS_PER_CUSTOMER", "from_field": "RISK_SCORE", "to_instance": "ROUTE_VALID_AND_HIGH_RISK", "to_field": "RISK_SCORE"},
        {"from_instance": "RANK_ORDERS_PER_CUSTOMER", "from_field": "CUSTOMER_ORDER_RANK", "to_instance": "ROUTE_VALID_AND_HIGH_RISK", "to_field": "CUSTOMER_ORDER_RANK"},
        {"from_instance": "ROUTE_VALID_AND_HIGH_RISK", "from_field": "ORDER_ID", "to_instance": "REFORMAT_PROFITABILITY_TARGET", "to_field": "ORDER_ID"},
        {"from_instance": "ROUTE_VALID_AND_HIGH_RISK", "from_field": "CUSTOMER_ID", "to_instance": "REFORMAT_PROFITABILITY_TARGET", "to_field": "CUSTOMER_ID"},
        {"from_instance": "ROUTE_VALID_AND_HIGH_RISK", "from_field": "CUSTOMER_SEGMENT", "to_instance": "REFORMAT_PROFITABILITY_TARGET", "to_field": "CUSTOMER_SEGMENT"},
        {"from_instance": "ROUTE_VALID_AND_HIGH_RISK", "from_field": "ORDER_AMOUNT", "to_instance": "REFORMAT_PROFITABILITY_TARGET", "to_field": "ORDER_AMOUNT"},
        {"from_instance": "ROUTE_VALID_AND_HIGH_RISK", "from_field": "TOTAL_COST", "to_instance": "REFORMAT_PROFITABILITY_TARGET", "to_field": "TOTAL_COST"},
        {"from_instance": "ROUTE_VALID_AND_HIGH_RISK", "from_field": "CUSTOMER_ORDER_RANK", "to_instance": "REFORMAT_PROFITABILITY_TARGET", "to_field": "CUSTOMER_ORDER_RANK"},
        {"from_instance": "REFORMAT_PROFITABILITY_TARGET", "from_field": "ORDER_ID", "to_instance": "UPDATE_PROFITABILITY_TARGET", "to_field": "ORDER_ID"},
        {"from_instance": "REFORMAT_PROFITABILITY_TARGET", "from_field": "GROSS_MARGIN", "to_instance": "UPDATE_PROFITABILITY_TARGET", "to_field": "GROSS_MARGIN"},
        {"from_instance": "UPDATE_PROFITABILITY_TARGET", "from_field": "ORDER_ID", "to_instance": "TGT_ORDER_PROFITABILITY", "to_field": "ORDER_ID"},
        {"from_instance": "UPDATE_PROFITABILITY_TARGET", "from_field": "CUSTOMER_ID", "to_instance": "TGT_ORDER_PROFITABILITY", "to_field": "CUSTOMER_ID"},
        {"from_instance": "UPDATE_PROFITABILITY_TARGET", "from_field": "CUSTOMER_SEGMENT", "to_instance": "TGT_ORDER_PROFITABILITY", "to_field": "CUSTOMER_SEGMENT"},
        {"from_instance": "UPDATE_PROFITABILITY_TARGET", "from_field": "ORDER_AMOUNT", "to_instance": "TGT_ORDER_PROFITABILITY", "to_field": "ORDER_AMOUNT"},
        {"from_instance": "UPDATE_PROFITABILITY_TARGET", "from_field": "GROSS_MARGIN", "to_instance": "TGT_ORDER_PROFITABILITY", "to_field": "GROSS_MARGIN"},
        {"from_instance": "UPDATE_PROFITABILITY_TARGET", "from_field": "CUSTOMER_ORDER_RANK", "to_instance": "TGT_ORDER_PROFITABILITY", "to_field": "CUSTOMER_ORDER_RANK"},
        {"from_instance": "UPDATE_PROFITABILITY_TARGET", "from_field": "LOAD_DATE", "to_instance": "TGT_ORDER_PROFITABILITY", "to_field": "LOAD_DATE"},
        {"from_instance": "ROUTE_VALID_AND_HIGH_RISK", "from_field": "ORDER_ID", "to_instance": "REFORMAT_REJECT_TARGET", "to_field": "ORDER_ID"},
        {"from_instance": "ROUTE_VALID_AND_HIGH_RISK", "from_field": "CUSTOMER_ID", "to_instance": "REFORMAT_REJECT_TARGET", "to_field": "CUSTOMER_ID"},
        {"from_instance": "REFORMAT_REJECT_TARGET", "from_field": "ORDER_ID", "to_instance": "TGT_ORDER_REJECTS", "to_field": "ORDER_ID"},
        {"from_instance": "REFORMAT_REJECT_TARGET", "from_field": "CUSTOMER_ID", "to_instance": "TGT_ORDER_REJECTS", "to_field": "CUSTOMER_ID"},
        {"from_instance": "REFORMAT_REJECT_TARGET", "from_field": "REJECT_REASON", "to_instance": "TGT_ORDER_REJECTS", "to_field": "REJECT_REASON"},
        {"from_instance": "REFORMAT_REJECT_TARGET", "from_field": "LOAD_DATE", "to_instance": "TGT_ORDER_REJECTS", "to_field": "LOAD_DATE"}
      ]
    }
  ]
}
