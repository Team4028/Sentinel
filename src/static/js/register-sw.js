// ==
// much thanks to: https://suryasankar.medium.com/how-to-setup-basic-web-push-notification-functionality-using-a-flask-backend-1251a5413bbe

/**
 * Name is self-explanatory, converts a base64url string into a char[]
 */
function urlB64ToUint8Array(base64String) {
    const padding = '='.repeat((4 - base64String.length % 4) % 4);
    const base64 = (base64String + padding)
        .replace(/\-/g, '+')
        .replace(/_/g, '/'); // convert b64url to b64

    const rawData = window.atob(base64); // convert b64 to string
    const outputArray = new Uint8Array(rawData.length);

    for (let i = 0; i < rawData.length; ++i) {
        outputArray[i] = rawData.charCodeAt(i); // str -> char[]
    }
    return outputArray;
}

function updateSubscriptionOnServer(subscription, apiEndpoint) {
    return fetch(apiEndpoint, { // send sub data to backend so it can send you things (like Mr. Beast)
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': csrfToken
        },
        body: JSON.stringify({
            subscription_json: JSON.stringify(subscription)
        })
    });

}

function subscribeUser(swRegistration, applicationServerPublicKey, apiEndpoint) {
    const applicationServerKey = urlB64ToUint8Array(applicationServerPublicKey);
    swRegistration.pushManager.subscribe({
        userVisibleOnly: true,
        applicationServerKey: applicationServerKey
    })
        .then(function (subscription) {
            console.log('User is subscribed.');
            return updateSubscriptionOnServer(subscription, apiEndpoint);
        })
        .catch(function (err) {
            console.log('Failed to subscribe the user: ', err);
            console.log(err.stack);
        });
}

function registerServiceWorker(serviceWorkerUrl, applicationServerPublicKey, apiEndpoint) {
    let swRegistration = null;
    if ('serviceWorker' in navigator && 'PushManager' in window) {
        console.log('Service Worker and Push is supported');

        navigator.serviceWorker.register(serviceWorkerUrl); // make SW
        navigator.serviceWorker.ready.then(function (swReg) { // when it is done being created...
            console.log('Service Worker is registered', swReg);
            swReg.active.postMessage({
                type: "SET_CSRF",
                token: csrfToken
            });
            subscribeUser(swReg, applicationServerPublicKey, apiEndpoint); // subscribe to the server
            swRegistration = swReg;
        })
            .catch(function (error) {
                console.error('Service Worker Error', error);
            });
    } else {
        console.warn('Push messaging is not supported');
    }
    return swRegistration;
}