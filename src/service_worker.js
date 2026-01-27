'use strict';

let csrfToken = null;
let cid = null;

self.addEventListener("message", event => {
    if (event.data.type == "SET_CSRF") {
        if (!csrfToken || !cid) {
            csrfToken = event.data.token;
            cid = event.data.cid;
            console.log("Service Worker recieved CSRF + Client ID");
        }
    }
});

self.addEventListener('install', function (event) {
    console.log('Service Worker installing.');
});

self.addEventListener('activate', function (event) {
    console.log('Service Worker activating.');
});

self.addEventListener("notificationclick", event => {
    console.log(`Clicked ${event.action}`); // goto-changes or remove-change, defined in app.py in the webpush data
    if (event.action === "goto-changes") { // go to /changes endpoint:
        event.waitUntil(
            clients.matchAll({ type: "window" }).then(cList => {
                for (const client of cList) {
                    console.log(client);
                    // if /changes is already open, go to that page and reload it
                    if (client.url === new URL('/changes', self.location.href).toString() && "focus" in client) {
                        client.navigate(client.url);
                        return client.focus();
                    }
                }
                // else, open /changes
                if (clients.openWindow) return clients.openWindow("/changes");
            })
        );
    } else if (event.action === "remove-change") {
        fetch("/delete-lines", { // use stored notification data from webpush in flask to delete these lines
            method: "POST",
            credentials: 'include',
            headers: {
                "Content-Type": "application/json",
                'X-CSRFToken': csrfToken
            },
            "body": JSON.stringify({ lines: JSON.parse(event.notification.data["line-hashes"]) }) // parse and then unparse the json string
        }).then(res => {
            if (res.status == 400) { // assume that malformed request is due to csrf because the request json is guarenteed bc we're sending it 
                self.registration.showNotification("Error", {
                    body: "CSRF invalid, please reload page",
                });
            } else if (res.status == 401 || res.status == 403) {
                self.registration.showNotification("Error", {
                    body: "Authentication error: are you logged in?"
                });
            } else if (res.status == 500) {
                res.text().then(txt => {
                    self.registration.showNotification("Error", {
                        body: `Internal Server error: ${txt}`
                    })
                })
            }
        });
    }
});

setInterval(() => {
    fetch("/notifyq", {
        method: "GET",
        credentials: 'include',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': csrfToken,
            'X-Cid': cid
        }
    }).then(res => {
        if (res.ok && res.status === 200) // do nothing if 204
            res.json().then(json => {
                console.log(`SEND ${JSON.stringify(json)}`);
                if ("title" in json) {
                    const title = json["title"];
                    delete json.title;
                    self.registration.showNotification(title, json);
                }
            });
    }).catch(() => {});
}, 1000);