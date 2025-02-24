CREATE TABLE nft_owners (
    owner           TEXT NOT NULL,
    contract        TEXT NOT NULL,
    token_id        TEXT NOT NULL,
    token_unique_id INTEGER NOT NULL,
    timestamp       INTEGER NOT NULL,
    PRIMARY KEY (contract, token_id, token_unique_id, owner),
    FOREIGN KEY (contract, token_id) REFERENCES nfts (contract, token_id) ON DELETE CASCADE
);

-- Index for contract & token lookups
CREATE INDEX idx_nft_owner_contract_token_unique_id ON nft_owners (contract, token_id, token_unique_id);

-- Index for looking up owners quickly
CREATE INDEX idx_nft_owner ON nft_owners (owner);

-- Index for timestamp
CREATE INDEX idx_nft_owner_timestamp ON nft_owners (timestamp);