use photon_indexer::openapi::update_docs;

#[test]
pub fn test_documentation_generation() {
    update_docs(true);
}
