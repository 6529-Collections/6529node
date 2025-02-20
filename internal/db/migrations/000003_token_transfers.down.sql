DROP INDEX IF EXISTS idx_tx_hash;
DROP INDEX IF EXISTS idx_contract;
DROP INDEX IF EXISTS idx_from;
DROP INDEX IF EXISTS idx_to;
DROP INDEX IF EXISTS idx_transfer_type;
DROP INDEX IF EXISTS idx_token_id;
DROP INDEX IF EXISTS idx_amount;

DROP TABLE IF EXISTS token_transfers;

DROP TABLE IF EXISTS token_transfers_checkpoint;