from etl_cc.gliner_service import gliner_service

def run_gliner_tests():
    print("--- Testing GLiNER Service ---")
    
    # 1. Test Failure Triage
    print("\n1. Failure Triage Test:")
    triage_res = gliner_service.triage_failure("Discovery", "Invalid token while connecting to repo")
    print(triage_res)

    # 2. Test Discovery Routing
    print("\n2. Discovery Routing Test:")
    discovery_res = gliner_service.decide_discovery_routing("Complexity: LOW. Risky transformations: 0. Unsupported constructs: 0. Has cycle: False.")
    print(discovery_res)

    # 3. Test Validation Routing
    print("\n3. Validation Routing Test:")
    validation_res = gliner_service.check_validation_uncertainty("Coverage: 95.0%. Oracle strength: STRONG. Checks passed: 12.")
    print(validation_res)

if __name__ == "__main__":
    run_gliner_tests()