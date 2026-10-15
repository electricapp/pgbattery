//! The one door into the Raft log.
//!
//! A command is admitted against the committed feature level before it is
//! proposed, so no member is ever sent an entry its binary cannot decode.
//! `testing/lint_propose.py` keeps `client_write` out of every other module.

use openraft::BasicNode;
use openraft::error::{ClientWriteError, RaftError};
use parking_lot::RwLock;

use super::raft::{ClusterRequest, ClusterResponse, TypeConfig};
use super::state_machine::{ClusterCommand, ClusterState, CommandOutcome, NodeId};

/// Why a proposal did not commit.
#[derive(Debug)]
pub enum ProposeError {
    /// The committed feature level does not admit the command yet.
    NotAdmitted {
        required: u32,
        committed: u32,
    },
    /// The level rises only through [`finalize_feature_level`].
    RaisesLevel,
    /// Some member's support for the next level is unknown or too low.
    MembersNotReady(String),
    Raft(Box<RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>>),
}

impl std::fmt::Display for ProposeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotAdmitted {
                required,
                committed,
            } => write!(
                f,
                "the cluster has committed feature level {committed} and this command \
                 needs {required}; finish the rolling upgrade with \
                 `pgbattery cluster finalize-upgrade`"
            ),
            Self::RaisesLevel => write!(
                f,
                "the feature level rises only through finalize, once every member supports it"
            ),
            Self::MembersNotReady(why) => write!(f, "{why}"),
            Self::Raft(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for ProposeError {}

impl From<RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>> for ProposeError {
    fn from(e: RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>) -> Self {
        Self::Raft(Box::new(e))
    }
}

/// Whether the committed level admits `command`.
///
/// # Errors
/// [`ProposeError::NotAdmitted`] above the committed level;
/// [`ProposeError::RaisesLevel`] for `SetFeatureLevel`.
pub const fn admit(state: &ClusterState, command: &ClusterCommand) -> Result<(), ProposeError> {
    if matches!(command, ClusterCommand::SetFeatureLevel { .. }) {
        return Err(ProposeError::RaisesLevel);
    }
    let required = command.required_feature_level();
    if required > state.feature_level {
        return Err(ProposeError::NotAdmitted {
            required,
            committed: state.feature_level,
        });
    }
    Ok(())
}

/// Admit `command` against the committed level, then commit it.
///
/// # Errors
/// Refused admission, or the Raft write's own failure.
pub async fn propose(
    raft: &openraft::Raft<TypeConfig>,
    state: &RwLock<ClusterState>,
    command: ClusterCommand,
) -> Result<ClusterResponse, ProposeError> {
    admit(&state.read(), &command)?;
    Ok(raft.client_write(ClusterRequest { command }).await?.data)
}

/// The level to raise to, given each member's reported support.
///
/// `None` support is a member that did not answer, and it blocks the raise:
/// committing a level a member cannot decode stalls its replication at the
/// first gated entry. `Ok(None)` means the committed level is already the
/// highest every member supports.
///
/// # Errors
/// [`ProposeError::MembersNotReady`] naming each silent member.
pub fn finalize_target(
    committed: u32,
    members: &[(NodeId, Option<u32>)],
) -> Result<Option<u32>, ProposeError> {
    let silent: Vec<NodeId> = members
        .iter()
        .filter(|(_, level)| level.is_none())
        .map(|(id, _)| *id)
        .collect();
    if !silent.is_empty() {
        return Err(ProposeError::MembersNotReady(format!(
            "cannot raise the feature level: members {silent:?} did not report the level \
             they support"
        )));
    }
    let common = members.iter().filter_map(|(_, level)| *level).min();
    Ok(common.filter(|&level| level > committed))
}

/// What finalize did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Finalized {
    /// Already at the highest level every member supports.
    AlreadyAt(u32),
    Raised(u32),
}

/// Raise the committed level to the highest every member supports.
///
/// The caller supplies every member's support, learners included, and must
/// hold the membership lock so no member joins between the survey and the
/// commit.
///
/// # Errors
/// A silent member, or the Raft write's own failure.
pub async fn finalize_feature_level(
    raft: &openraft::Raft<TypeConfig>,
    state: &RwLock<ClusterState>,
    members: &[(NodeId, Option<u32>)],
) -> Result<Finalized, ProposeError> {
    let committed = state.read().feature_level;
    let Some(level) = finalize_target(committed, members)? else {
        return Ok(Finalized::AlreadyAt(committed));
    };
    let response = raft
        .client_write(ClusterRequest {
            command: ClusterCommand::SetFeatureLevel { level },
        })
        .await?;
    Ok(match response.data.outcome {
        CommandOutcome::Applied => Finalized::Raised(level),
        CommandOutcome::OutOfOrder => Finalized::AlreadyAt(state.read().feature_level),
    })
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use super::*;
    use crate::governor::state_machine::FEATURE_LEVEL;

    fn arm() -> ClusterCommand {
        ClusterCommand::CutoverArmed {
            target_cluster_id: "green".into(),
            local_cluster_id: "blue".into(),
        }
    }

    #[test]
    fn a_gated_command_is_not_proposed_below_its_level() {
        let state = ClusterState::default();
        assert!(matches!(
            admit(&state, &arm()),
            Err(ProposeError::NotAdmitted {
                required: 1,
                committed: 0
            })
        ));
        assert!(admit(&state, &ClusterCommand::SetSyncMode { active: true }).is_ok());
    }

    #[test]
    fn a_gated_command_is_proposed_at_its_level() {
        let state = ClusterState {
            feature_level: FEATURE_LEVEL,
            ..ClusterState::default()
        };
        assert!(admit(&state, &arm()).is_ok());
    }

    #[test]
    fn the_level_is_never_raised_through_propose() {
        let state = ClusterState::default();
        assert!(matches!(
            admit(&state, &ClusterCommand::SetFeatureLevel { level: 1 }),
            Err(ProposeError::RaisesLevel)
        ));
    }

    #[test]
    fn finalize_raises_to_the_lowest_member_support() {
        let members = [(1, Some(2)), (2, Some(1)), (3, Some(2))];
        assert_eq!(finalize_target(0, &members).unwrap(), Some(1));
        assert_eq!(finalize_target(1, &members).unwrap(), None);
    }

    #[test]
    fn a_silent_member_blocks_finalize() {
        let members = [(1, Some(1)), (2, None), (3, Some(1))];
        let refused = finalize_target(0, &members).unwrap_err();
        assert!(refused.to_string().contains("[2]"), "{refused}");
    }

    #[test]
    fn an_old_binary_holds_the_level_at_zero() {
        let members = [(1, Some(1)), (2, Some(0)), (3, Some(1))];
        assert_eq!(finalize_target(0, &members).unwrap(), None);
    }
}
