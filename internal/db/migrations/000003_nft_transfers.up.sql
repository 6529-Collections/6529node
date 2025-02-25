CREATE TABLE nft_transfers (
    block_number      INTEGER NOT NULL,
    transaction_index INTEGER NOT NULL,
    log_index         INTEGER NOT NULL,
    block_time        INTEGER NOT NULL,
    tx_hash           TEXT NOT NULL,
    event_name        TEXT NOT NULL,
    from_address      TEXT NOT NULL,
    to_address        TEXT NOT NULL,
    contract          TEXT NOT NULL,
    token_id          TEXT NOT NULL,
    token_unique_id   INTEGER NOT NULL,
    transfer_type     TEXT NOT NULL CHECK (transfer_type IN ('SALE', 'SEND', 'AIRDROP', 'MINT', 'BURN')),
    PRIMARY KEY       (tx_hash, log_index, from_address, to_address, contract, token_id, token_unique_id)
);

-- Index for fast lookups on transaction hash
CREATE INDEX idx_tx_hash ON nft_transfers (tx_hash);

-- Index for contract address lookups
CREATE INDEX idx_contract ON nft_transfers (contract);

-- Index for sender address lookups
CREATE INDEX idx_from ON nft_transfers (from_address);

-- Index for receiver address lookups
CREATE INDEX idx_to ON nft_transfers (to_address);

-- Index for transfer type to optimize queries filtering by type
CREATE INDEX idx_transfer_type ON nft_transfers (transfer_type);

-- Index for token ID for efficient filtering of specific tokens
CREATE INDEX idx_token_id ON nft_transfers (token_id);

-- Index for token unique ID for efficient filtering of specific tokens
CREATE INDEX idx_token_unique_id ON nft_transfers (token_unique_id);

CREATE TABLE token_transfers_checkpoint (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    block_number INTEGER NOT NULL,
    transaction_index INTEGER NOT NULL,
    log_index INTEGER NOT NULL
);
