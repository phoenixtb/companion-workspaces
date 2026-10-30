// Part 31: Cube's configuration (the model is in model/). Cube never decides who sees which rows: it connects to
// Trino as a named identity and OPA (Part 23) filters and masks for that identity, exactly as for a person.
//
// A request's signed token (REST API) carries a `scope`: `all` for head-office tools, or one country for a
// country's app. The scope picks the Trino user. The SQL API (Superset) has no token, so it gets the default scope.
const TRINO_USERS = {
  all: 'cube', // opa/larkspur/users/data.json: an analyst-class identity for GB, IN and SG
  GB: 'analyst_gb',
  IN: 'analyst_in',
  SG: 'analyst_sg',
};

const trinoUser = (securityContext) => {
  const scope = (securityContext && securityContext.scope) || 'all';
  const user = TRINO_USERS[scope];
  if (!user) {
    throw new Error(`unknown scope: ${scope}`);
  }
  return user;
};

// The post's Steps 4 and 6 switch two safeguards off to show what they prevent: LAB_MISTAKES=shared-orchestrator
// and/or client-timezone (compose.yaml passes it through; empty by default).
const mistakes = (process.env.LAB_MISTAKES || '').split(',');

module.exports = {
  // The app id keys the compiled model; the orchestrator id keys the query orchestrator: the database connections
  // (driverFactory runs once per orchestrator id) and the result cache. The default is one orchestrator for every
  // tenant, so the first identity's connection, and its cached results, would serve every scope.
  contextToAppId: ({ securityContext }) => `larkspur_${trinoUser(securityContext)}`,
  ...(mistakes.includes('shared-orchestrator')
    ? {}
    : { contextToOrchestratorId: ({ securityContext }) => `trino_${trinoUser(securityContext)}` }),

  driverFactory: ({ securityContext }) => ({ type: 'trino', user: trinoUser(securityContext) }),

  // Business dates are calendar dates, already local to each store (Part 5). Cube converts time dimensions into the
  // query's time zone, and a zone west of UTC moves every date back a day. The server decides: queries run in UTC.
  ...(mistakes.includes('client-timezone') ? {} : { queryRewrite: (query) => ({ ...query, timezone: 'UTC' }) }),

  // No pre-aggregations in this lab, so the refresh worker has nothing to build; listing the scopes keeps its
  // background contexts defined.
  scheduledRefreshContexts: async () => Object.keys(TRINO_USERS).map((scope) => ({ securityContext: { scope } })),
};
