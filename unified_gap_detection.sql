-- Unified Gap Detection for Both State and Address Trees
-- Now that both tree types are tracked in state_tree_histories

WITH 
-- Get all trees and their sequence ranges
tree_ranges AS (
    SELECT 
        tree,
        MIN(seq) as min_seq,
        MAX(seq) as max_seq,
        COUNT(DISTINCT seq) as distinct_seq_count
    FROM state_tree_histories
    GROUP BY tree
),

-- Generate expected sequence numbers for each tree
expected_sequences AS (
    SELECT 
        tree,
        generate_series(min_seq::int, max_seq::int) as expected_seq
    FROM tree_ranges
),

-- Find missing sequences
gaps AS (
    SELECT 
        e.tree,
        e.expected_seq as missing_seq
    FROM expected_sequences e
    LEFT JOIN state_tree_histories sth 
        ON sth.tree = e.tree 
        AND sth.seq = e.expected_seq
    WHERE sth.seq IS NULL
),

-- Group consecutive gaps into ranges
gap_ranges AS (
    SELECT 
        tree,
        MIN(missing_seq) as gap_start,
        MAX(missing_seq) as gap_end,
        COUNT(*) as gap_size
    FROM (
        SELECT 
            tree,
            missing_seq,
            missing_seq - DENSE_RANK() OVER (PARTITION BY tree ORDER BY missing_seq) as grp
        FROM gaps
    ) t
    GROUP BY tree, grp
),

-- Get slot information for gap boundaries
gap_slots AS (
    SELECT 
        gr.*,
        encode(gr.tree, 'base64') as tree_text,
        
        -- Last indexed slot before gap
        (SELECT t.slot
         FROM state_tree_histories sth
         JOIN transactions t ON t.signature = sth.transaction_signature
         WHERE sth.tree = gr.tree AND sth.seq = gr.gap_start - 1
         LIMIT 1
        ) as last_indexed_slot,
        
        -- First indexed slot after gap
        (SELECT t.slot
         FROM state_tree_histories sth
         JOIN transactions t ON t.signature = sth.transaction_signature
         WHERE sth.tree = gr.tree AND sth.seq = gr.gap_end + 1
         LIMIT 1
        ) as next_indexed_slot,
        
        -- Transaction signatures for debugging
        encode((SELECT sth.transaction_signature
               FROM state_tree_histories sth
               WHERE sth.tree = gr.tree AND sth.seq = gr.gap_start - 1
               LIMIT 1), 'base64') as last_tx_signature,
               
        encode((SELECT sth.transaction_signature
               FROM state_tree_histories sth
               WHERE sth.tree = gr.tree AND sth.seq = gr.gap_end + 1
               LIMIT 1), 'base64') as next_tx_signature
    FROM gap_ranges gr
)

SELECT 
    tree_text,
    gap_start,
    gap_end,
    gap_size,
    last_indexed_slot,
    next_indexed_slot,
    (last_indexed_slot + 1) as reindex_start_slot,
    (COALESCE(next_indexed_slot, (SELECT MAX(slot) FROM transactions)) - 1) as reindex_end_slot,
    last_tx_signature,
    next_tx_signature
FROM gap_slots
WHERE last_indexed_slot IS NOT NULL
ORDER BY gap_size DESC, last_indexed_slot ASC
LIMIT 50;