const http = require("http");
const { Client } = require("pg");
const fs = require("fs");
const config = require("config");

const delayTime = config.get("delay");

const URLS = {
  restaurantService: "restaurant-service.preprod.thefork.io",
  customerService: "customer-service.preprod.thefork.io",
  reservationService: "reservation-service.preprod.thefork.io",
};

handle("./queries");

async function handle(dirName) {
  var nbCustomers = 0;
  const fileContent = fs.readFileSync(dirName + "/group_uuids.json", "utf8");
  for (const groupUuid of JSON.parse(fileContent)) {
    var now = new Date();
    console.log(nbCustomers);
    console.log("----------------------- GROUP ---------------------------");
    console.log("----------------------- " + groupUuid);
    console.log("Starting group at " + now.toUTCString());
    // convert date to a string in UTC timezone format:
    const restaurantUuids = await getRestaurantUuids(groupUuid);
    const customerUuids = await getCustomers(groupUuid);
    console.log("NB OF CUSTOMER");
    console.log(customerUuids.length);
    nbCustomers = nbCustomers + customerUuids.length;
    customerUuids.forEach(async (customerUuid) => {
      if (customerUuid.uuids !== null) {
        const reservations = await getReservations(
          customerUuid.uuids,
          restaurantUuids
        );
      }
    });
    var now = new Date();
    console.log("ending group at " + now.toUTCString());
  }

  console.log(nbCustomers);

  await delay();
}

async function getReservations(customerUuids, restaurantUuids) {
  const data = await call(
    URLS.reservationService,
    "",
    "countReservationsAndLastReservationMealDateByCustomer",
    {
      customerUuids: customerUuids,
      restaurantUuids: restaurantUuids,
    }
  );

  return data;
}

/**
 * Fetch the list of offlineCustomers of a group
 */
async function getCustomers(groupUuid) {
  const data = await call(
    URLS.customerService,
    "/json-rpc",
    "searchOfflineCustomer",
    {
      ownerUuid: groupUuid,
      metadata: {
        limit: 500000,
      },
    }
  );

  if (!data.result.customers) {
    return [];
  }

  const result = data.result.customers.map((res) => {
    if (res.onlineCustomerUuid) {
      return {
        uuids: [res.customerUuid, res.onlineCustomerUuid],
      };
    }

    return {
      uuids: [res.customerUuid],
    };
  });

  return result;
}

/**
 * Fetch every restaurantUuids of one group
 */
async function getRestaurantUuids(groupUuid) {
  const data = await call(
    URLS.restaurantService,
    "/json-rpc",
    "getRestaurantsByGroupUuid",
    {
      groupUuid,
    }
  );

  const result = data.result.map((res) => {
    return res.restaurantUuid;
  });

  return result;
}

/**
 * Json Rpc call responsibility
 */
async function call(host, path, method, params) {
  return new Promise(function (resolve, reject) {
    const httpOptions = {
      hostname: host,
      port: "80",
      path,
      method: "POST",
      headers: { "Content-Type": "application/json; charset=utf-8" },
    };

    var req = http.request(httpOptions, function (res) {
      if (res.statusCode < 200 || res.statusCode >= 300) {
        return reject(new Error("statusCode=" + res.statusCode));
      }

      var body = [];
      res.on("data", function (chunk) {
        body.push(chunk);
      });

      res.on("end", function () {
        try {
          body = JSON.parse(Buffer.concat(body).toString());
        } catch (e) {
          reject(e);
        }
        resolve(body);
      });
    });

    req.on("error", function (err) {
      reject(err);
    });

    req.write(
      JSON.stringify({
        jsonrpc: "2.0",
        id: "ID321",
        method: method,
        params: params,
      })
    );

    req.end();
  });
}

function delay() {
  return new Promise((resolve) => setTimeout(resolve, delayTime));
}
