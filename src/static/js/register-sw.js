let cid = localStorage.getItem("client_id");
let callbackId;
let checkId;
if (!cid) {
    cid = crypto.randomUUID();
    localStorage.setItem("client_id", cid);
}

function sendCSRF(worker) {
    worker.postMessage({
        type: "SET_CSRF",
        token: csrfToken,
        cid: cid
    });
}

function checkCSRF(worker) {
    worker.postMessage({
        type: "CHECK",
    });
}

function registerServiceWorker(serviceWorkerUrl) {
    let swRegistration = null;
    if ('serviceWorker' in navigator && 'PushManager' in window) {
        console.log('Service Worker and Push is supported');
        navigator.serviceWorker.register(serviceWorkerUrl); // make SW
        navigator.serviceWorker.ready.then(function (swReg) { // when it is done being created...
            console.log('Service Worker is registered', swReg);
            callbackId = setInterval(() => { console.log("sending CSRF"); sendCSRF(swReg.active); }, 100);
            swRegistration = swReg;
            navigator.serviceWorker.addEventListener("message", (e) => {
                if (e.data.type === "RECIEVED_CSRF") {
                    clearInterval(callbackId);
                    checkId = setInterval(() => { console.log("Checking CSRF"); checkCSRF(swReg.active); }, 30000);
                }
                
                else if (e.data.type === "CHECK_RES") {
                    if (e.data.good === "false") {
                        callbackId = setInterval(() => { console.log("sending CSRF"); sendCSRF(swReg.active); }, 100);
                        clearInterval(checkId);
                    }
                }
            })
        })
            .catch(function (error) {
                console.error('Service Worker Error', error);
            });
    } else {
        console.warn('Push messaging is not supported');
    }
    return swRegistration;
}