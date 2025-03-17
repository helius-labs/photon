use lazy_static::lazy_static;
use light_compressed_account::TreeType;
use solana_program::pubkey;
use solana_program::pubkey::Pubkey;
use std::collections::HashMap;

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
            "EzKE84aVTkCUhDHLELqyJaq1Y7UVVmqxXqZjVHwHY3rK".to_string(),
            TreeInfo {
                tree: pubkey!("EzKE84aVTkCUhDHLELqyJaq1Y7UVVmqxXqZjVHwHY3rK"),
                queue: pubkey!("EzKE84aVTkCUhDHLELqyJaq1Y7UVVmqxXqZjVHwHY3rK"),
                height: 40,
                tree_type: TreeType::BatchedAddress,
            },
        );

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
            "HLKs5NJ8FXkJg8BrzJt56adFYYuwg5etzDtBbQYTsixu".to_string(),
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

        m.insert(
            "EzKE84aVTkCUhDHLELqyJaq1Y7UVVmqxXqZjVHwHY3rK".to_string(),
            TreeInfo {
                tree: pubkey!("EzKE84aVTkCUhDHLELqyJaq1Y7UVVmqxXqZjVHwHY3rK"),
                queue: pubkey!("EzKE84aVTkCUhDHLELqyJaq1Y7UVVmqxXqZjVHwHY3rK"),
                height: 40,
                tree_type: TreeType::BatchedAddress,
            },
        );

        m
    };
}
