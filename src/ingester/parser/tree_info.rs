use lazy_static::lazy_static;
use light_merkle_tree_metadata::merkle_tree::TreeType;
use solana_program::pubkey;
use solana_program::pubkey::Pubkey;
use std::collections::HashMap;

pub const DEFAULT_TREE_HEIGHT: u32 = 32 + 1;

#[derive(Debug, Clone)]
pub struct TreeInfo {
    pub tree: Pubkey,
    pub queue: Pubkey,
    pub height: u32,
    pub tree_type: TreeType,
}

impl TreeInfo {
    pub fn get(pubkey: &str) -> Option<&TreeInfo> {
        QUEUE_TREE_MAPPING.get(pubkey)
    }

    pub fn height(pubkey: &str) -> Option<u32> {
        QUEUE_TREE_MAPPING.get(pubkey).map(|x| x.height + 1)
    }
}

// TODO: add a table which stores tree metadata: tree_pubkey | queue_pubkey | type | ...
lazy_static! {
    pub static ref QUEUE_TREE_MAPPING: HashMap<String, TreeInfo> = {
        let mut m = HashMap::new();

        m.insert(
            "6L7SzhYB3anwEQ9cphpJ1U7Scwj57bx2xueReg7R9cKU".to_string(),
            TreeInfo {
                tree: pubkey!("HLKs5NJ8FXkJg8BrzJt56adFYYuwg5etzDtBbQYTsixu"),
                queue: pubkey!("6L7SzhYB3anwEQ9cphpJ1U7Scwj57bx2xueReg7R9cKU"),
                height: 32,
                tree_type: TreeType::BatchedState,
            },
        );

        m.insert(
            "smt1NamzXdq4AMqS2fS2F1i5KTYPZRhoHgWx38d8WsT".to_string(),
            TreeInfo {
                tree: pubkey!("smt1NamzXdq4AMqS2fS2F1i5KTYPZRhoHgWx38d8WsT"),
                queue: pubkey!("nfq1NvQDJ2GEgnS8zt9prAe8rjjpAW1zFkrvZoBR148"),
                height: 26,
                tree_type: TreeType::State,
            },
        );

        m.insert(
            "smt2rJAFdyJJupwMKAqTNAJwvjhmiZ4JYGZmbVRw1Ho".to_string(),
            TreeInfo {
                tree: pubkey!("smt2rJAFdyJJupwMKAqTNAJwvjhmiZ4JYGZmbVRw1Ho"),
                queue: pubkey!("nfq2hgS7NYemXsFaFUCe3EMXSDSfnZnAe27jC6aPP1X"),
                height: 26,
                tree_type: TreeType::State,
            },
        );

        // TODO: update queue pubkeys
        //  m.insert(
        //     "smt3AFtReRGVcrP11D6bSLEaKdUmrGfaTNowMVccJeu".to_string(),
        //     TreeAndQueue {
        //         tree: pubkey!("smt3AFtReRGVcrP11D6bSLEaKdUmrGfaTNowMVccJeu"),
        //         queue: pubkey!("nfq2hgS7NYemXsFaFUCe3EMXSDSfnZnAe27jC6aPP1X"),
        //         height: 26,
        //         tree_type: TreeType::State,
        //     },
        // );
        //
        //  m.insert(
        //     "smt4vjXvdjDFzvRMUxwTWnSy4c7cKkMaHuPrGsdDH7V".to_string(),
        //     TreeAndQueue {
        //         tree: pubkey!("smt2rJAFdyJJupwMKAqTNAJwvjhmiZ4JYGZmbVRw1Ho"),
        //         queue: pubkey!("smt4vjXvdjDFzvRMUxwTWnSy4c7cKkMaHuPrGsdDH7V"),
        //         height: 26,
        //         tree_type: TreeType::State,
        //     },
        // );
        //
        //  m.insert(
        //     "smt5uPaQT9n6b1qAkgyonmzRxtuazA53Rddwntqistc".to_string(),
        //     TreeAndQueue {
        //         tree: pubkey!("smt5uPaQT9n6b1qAkgyonmzRxtuazA53Rddwntqistc"),
        //         queue: pubkey!("nfq2hgS7NYemXsFaFUCe3EMXSDSfnZnAe27jC6aPP1X"),
        //         height: 26,
        //         tree_type: TreeType::State,
        //     },
        // );
        //
        //  m.insert(
        //     "smt6ukQDSPPYHSshQovmiRUjG9jGFq2hW9vgrDFk5Yz".to_string(),
        //     TreeAndQueue {
        //         tree: pubkey!("smt6ukQDSPPYHSshQovmiRUjG9jGFq2hW9vgrDFk5Yz"),
        //         queue: pubkey!("nfq2hgS7NYemXsFaFUCe3EMXSDSfnZnAe27jC6aPP1X"),
        //         height: 26,
        //         tree_type: TreeType::State,
        //     },
        // );
        //
        //  m.insert(
        //     "smt7onMFkvi3RbyhQCMajudYQkB1afAFt9CDXBQTLz6".to_string(),
        //     TreeAndQueue {
        //         tree: pubkey!("smt7onMFkvi3RbyhQCMajudYQkB1afAFt9CDXBQTLz6"),
        //         queue: pubkey!("nfq2hgS7NYemXsFaFUCe3EMXSDSfnZnAe27jC6aPP1X"),
        //         height: 26,
        //         tree_type: TreeType::State,
        //     },
        // );
        //
        //  m.insert(
        //     "smt8TYxNy8SuhAdKJ8CeLtDkr2w6dgDmdz5ruiDw9Y9".to_string(),
        //     TreeAndQueue {
        //         tree: pubkey!("smt8TYxNy8SuhAdKJ8CeLtDkr2w6dgDmdz5ruiDw9Y9"),
        //         queue: pubkey!("nfq2hgS7NYemXsFaFUCe3EMXSDSfnZnAe27jC6aPP1X"),
        //         height: 26,
        //         tree_type: TreeType::State,
        //     },
        // );
        //
        //  m.insert(
        //     "smt9ReAYRF5eFjTd5gBJMn5aKwNRcmp3ub2CQr2vW7j".to_string(),
        //     TreeAndQueue {
        //         tree: pubkey!("smt9ReAYRF5eFjTd5gBJMn5aKwNRcmp3ub2CQr2vW7j"),
        //         queue: pubkey!("nfq2hgS7NYemXsFaFUCe3EMXSDSfnZnAe27jC6aPP1X"),
        //         height: 26,
        //         tree_type: TreeType::State,
        //     },
        // );
        //
        //  m.insert(
        //     "smtAvYA5UbTRyKAkAj5kHs1CmrA42t6WkVLi4c6mA1f".to_string(),
        //     TreeAndQueue {
        //         tree: pubkey!("smtAvYA5UbTRyKAkAj5kHs1CmrA42t6WkVLi4c6mA1f"),
        //         queue: pubkey!("nfq2hgS7NYemXsFaFUCe3EMXSDSfnZnAe27jC6aPP1X"),
        //         height: 26,
        //         tree_type: TreeType::State,
        //     },
        // );

         m.insert(
            "amt1Ayt45jfbdw5YSo7iz6WZxUmnZsQTYXy82hVwyC2".to_string(),
            TreeInfo {
                tree: pubkey!("amt1Ayt45jfbdw5YSo7iz6WZxUmnZsQTYXy82hVwyC2"),
                queue: pubkey!("aq1S9z4reTSQAdgWHGD2zDaS39sjGrAxbR31vxJ2F4F"),
                height: 26,
                tree_type: TreeType::Address,
            },
        );

         m.insert(
            "aq1S9z4reTSQAdgWHGD2zDaS39sjGrAxbR31vxJ2F4F".to_string(),
            TreeInfo {
                tree: pubkey!("amt1Ayt45jfbdw5YSo7iz6WZxUmnZsQTYXy82hVwyC2"),
                queue: pubkey!("aq1S9z4reTSQAdgWHGD2zDaS39sjGrAxbR31vxJ2F4F"),
                height: 26,
                tree_type: TreeType::Address,
            },
        );

        m.insert(
            "HLKs5NJ8FXkJg8BrzJt56adFYYuwg5etzDtBbQYTsixu".to_string(),
            TreeInfo {
                tree: pubkey!("HLKs5NJ8FXkJg8BrzJt56adFYYuwg5etzDtBbQYTsixu"),
                queue: pubkey!("6L7SzhYB3anwEQ9cphpJ1U7Scwj57bx2xueReg7R9cKU"),
                height: 32,
                tree_type: TreeType::BatchedState,
            },
        );

        m.insert(
            "nfq1NvQDJ2GEgnS8zt9prAe8rjjpAW1zFkrvZoBR148".to_string(),
            TreeInfo {
                tree: pubkey!("smt1NamzXdq4AMqS2fS2F1i5KTYPZRhoHgWx38d8WsT"),
                queue: pubkey!("nfq1NvQDJ2GEgnS8zt9prAe8rjjpAW1zFkrvZoBR148"),
                height: 26,
                tree_type: TreeType::State,
            },
        );

        m.insert(
            "nfq2hgS7NYemXsFaFUCe3EMXSDSfnZnAe27jC6aPP1X".to_string(),
            TreeInfo {
                tree: pubkey!("smt2rJAFdyJJupwMKAqTNAJwvjhmiZ4JYGZmbVRw1Ho"),
                queue: pubkey!("nfq2hgS7NYemXsFaFUCe3EMXSDSfnZnAe27jC6aPP1X"),
                height: 26,
                tree_type: TreeType::State,
            },
        );

        m
    };
}
