pub trait IndexedAccounts {
    fn get_accounts() -> &'static [&'static str];
    fn is_indexed_account(account: &str) -> bool {
        Self::get_accounts().contains(&account)
    }
}

pub struct Solayer;
impl IndexedAccounts for Solayer {
    fn get_accounts() -> &'static [&'static str] {
        &[
            "S1ay5sk6FVkvsNFZShMw2YK3nfgJZ8tpBBGuHWDZ266",
            "2sYfW81EENCMe415CPhE2XzBA5iQf4TXRs31W1KP63YT",
            "ARDPkhymCbfdan375FCgPnBJQvUfHeb7nHVdBfwWSxrp",
            "2sYfW81EENCMe415CPhE2XzBA5iQf4TXRs31W1KP63YT",
        ]
    }
}
