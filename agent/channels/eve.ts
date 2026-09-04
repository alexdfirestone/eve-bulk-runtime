import { eveChannel } from "eve/channels/eve";
import { localDev, vercelOidc } from "eve/channels/auth";

export default eveChannel({
  auth: [
    // Authenticates calls from this Vercel project, including the bulk workflow.
    vercelOidc(),
    // Accepted only under `eve dev`/`vercel dev`; it cannot open production.
    localDev(),
  ],
});
