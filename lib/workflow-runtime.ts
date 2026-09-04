import { getWorld } from "workflow/runtime";

let coordinatorWorld: Promise<void> | undefined;

/**
 * Initialize the parent Workflow SDK before using its lazy API helpers.
 *
 * withEve runs its own vendored Workflow world in a separate service. During
 * local Next.js development both packages register lazy world accessors on the
 * host process, so eagerly resolving the coordinator's public Workflow world
 * prevents the eve accessor from being selected for application workflows.
 */
export function ensureCoordinatorWorkflowWorld() {
  coordinatorWorld ??= getWorld().then(() => undefined).catch((error) => {
    coordinatorWorld = undefined;
    throw error;
  });
  return coordinatorWorld;
}
