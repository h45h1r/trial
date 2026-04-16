module.exports = {
  apps: [{
    name: "telemachus",
    script: "server.js",
    env: {
      SUPABASE_DSN: "postgresql://postgres:NkpLKDbnZvrvmnTD@db.rnxqdcylvswfbvxgspuq.supabase.co:5432/postgres"
    }
  }]
};
