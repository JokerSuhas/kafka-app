const { kafka } = require('./client');

const group = process.argv[2];

async function init() {
    const consumer = kafka.consumer({ groupId: group });
    await consumer.connect();

    await consumer.subscribe({ topic: 'rider-updates', fromBeginning: true }); // Corrected topic name and object syntax

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => { // Fixed syntax for eachMessage function
            console.log(`${group}: [${topic}]: PART:${partition}:`, message.value.toString()); // Fixed logging syntax
        }
    });
}

init();
