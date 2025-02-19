DROP INDEX IF EXISTS idx_nft_contract;
DROP INDEX IF EXISTS idx_nft_token_id;
DROP INDEX IF EXISTS idx_nft_supply;
DROP INDEX IF EXISTS idx_nft_burnt_supply;

DROP TABLE IF EXISTS nfts;


DROP TRIGGER IF EXISTS enforce_singleton;
DROP TABLE IF EXISTS token_transfers_checkpoint;