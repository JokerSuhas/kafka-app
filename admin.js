const { kafka } = require("./client");

async function init() {
    let admin; // Define admin outside try block

    try {
        admin = kafka.admin();
        console.log('Admin connecting');

        await admin.connect();
        console.log('Success');

        console.log('Creating Topic [rider-updates]');

        await admin.createTopics({
            topics: [{
                topic: 'rider-updates',
                numPartitions: 2,
                replicationFactor: 1
            }]
        });

        console.log('Topic [rider-updates] created');
    } catch (error) {
        console.error('Error occurred:', error);
    } finally {
        if (admin) {
            await admin.disconnect();
        }
    }
}

init();
