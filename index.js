const { Client } = require("pg");
const fs = require("fs");
const config = require("config");

const delayTime = config.get("delay");
const dbConfig = config.get("database");
const client = new Client(dbConfig);
client.connect();

handle("./queries", client);

async function handle(dirName, client) {
  await fs.readdir(dirName, (err, items) => {
    items.forEach(async (fileName) => {
      const fileContent = fs.readFileSync(dirName + "/" + fileName);
      const queries = JSON.parse(fileContent)["queries"];

      for (const query of queries) {
        let res = await call(query, client);

        // Modify to suit your needs
        let queries = res.rows.map((row) => {
          console.log(row);
          let ps = "[" + row.lower_ps + ", 20]";
          return `UPDATE offer SET party_size = '${ps}' WHERE id = '${row.id}';\n`;
        });

        // Write results
        queries.forEach((query) => {
          fs.appendFileSync("results/" + fileName + ".txt", query);
        });

        await delay();
      }

      client.end();
    });
  });
}

async function call(query, client) {
  return await client.query(query);
}

function delay() {
  return new Promise((resolve) => setTimeout(resolve, delayTime));
}
