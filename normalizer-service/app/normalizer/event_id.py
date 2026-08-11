import hashlib

def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()

def create_block_event_id(
    chain_id: int,
    block_hash: str,
) -> str:
    identity = f"block|{chain_id}|{block_hash.lower()}"
    return _sha256(identity)

def create_transaction_event_id(
    chain_id: int,
    block_hash: str,
    transaction_hash: str,
) -> str:
    identity = (
        f"transaction|{chain_id}|"
        f"{block_hash.lower()}|"
        f"{transaction_hash.lower()}"
    )
    return _sha256(identity)

def create_log_event_id(
    chain_id: int,
    block_hash: str,
    transaction_hash: str,
    log_index: int,
) -> str:
    identity = (
        f"log|{chain_id}|"
        f"{block_hash.lower()}|"
        f"{transaction_hash.lower()}|"
        f"{log_index}"
    )
    return _sha256(identity)
