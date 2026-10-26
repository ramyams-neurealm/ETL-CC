from etl_cc.agents.spark_runtime import add_java_home_to_path


def test_java_home_bin_is_added_to_spark_child_path(tmp_path):
    java_bin = tmp_path / "bin"
    java_bin.mkdir()
    (java_bin / "java.exe").write_text("", encoding="utf-8")
    environment = {
        "JAVA_HOME": str(tmp_path),
        "PATH": "C:\\Windows\\System32",
    }

    add_java_home_to_path(environment)
    add_java_home_to_path(environment)

    entries = environment["PATH"].split(";")
    assert entries[0] == str(java_bin)
    assert entries.count(str(java_bin)) == 1
