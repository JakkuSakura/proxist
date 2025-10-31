use anyhow::bail;

/// Placeholder types for FerroPhase-backed SQL AST integration.
/// When a FerroPhase SQL frontend is available, wire it here.

#[derive(Debug, Clone)]
pub struct AstNode;

pub trait AstParser {
    fn parse(&self, sql: &str) -> anyhow::Result<AstNode>;
    fn print(&self, ast: &AstNode) -> String;
}

pub struct FerroPhaseSql;

impl AstParser for FerroPhaseSql {
    fn parse(&self, _sql: &str) -> anyhow::Result<AstNode> {
        bail!("FerroPhase SQL frontend not integrated yet")
    }
    fn print(&self, _ast: &AstNode) -> String {
        "<unimplemented>".into()
    }
}

