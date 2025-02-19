CREATE TABLE nft_owners (
    owner        TEXT NOT NULL,
    contract     TEXT NOT NULL,
    token_id     TEXT NOT NULL,
    balance      INTEGER NOT NULL CHECK (balance >= 0),
    PRIMARY KEY (contract, token_id, owner),
    FOREIGN KEY (contract, token_id) REFERENCES nfts (contract, token_id) ON DELETE CASCADE
);

-- Index for contract & token lookups
CREATE INDEX idx_nft_owner_contract_token ON nft_owners (contract, token_id);

-- Index for looking up owners quickly
CREATE INDEX idx_nft_owner ON nft_owners (owner);