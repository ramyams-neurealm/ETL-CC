from cryptography.fernet import Fernet

def test_key_format():
    assert len(Fernet.generate_key()) == 44
