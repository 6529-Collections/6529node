CREATE TABLE nfts (
    contract     TEXT NOT NULL,
    token_id     TEXT NOT NULL,
    supply       INTEGER NOT NULL CHECK (supply >= 0),
    burnt_supply INTEGER NOT NULL CHECK (burnt_supply >= 0),
    PRIMARY KEY (contract, token_id)
);

-- Index for contract lookups
CREATE INDEX idx_nft_contract ON nfts (contract);

-- Index for token ID lookups
CREATE INDEX idx_nft_token_id ON nfts (token_id);

-- Index for querying supply efficiently
CREATE INDEX idx_nft_supply ON nfts (supply);

-- Index for querying burnt supply efficiently
CREATE INDEX idx_nft_burnt_supply ON nfts (burnt_supply);