const kafka = require('kafka-node');

module.exports = function(RED) {
  function kafkaIn(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const { kafkaHost, topics, groupId, autoCommit, fetchMaxBytes, connectTimeout,delayConsumerCreation, debug} = config;


    let opt_groupId = "kafka-node-group";
    if(groupId)
      opt_groupId = groupId;

    let opt_fetchMaxBytes = 1024*1024;
    if(fetchMaxBytes){
      opt_fetchMaxBytes = parseInt(fetchMaxBytes);
    }

    const consumerOptions = {
      autoCommit:autoCommit,
      groupId: opt_groupId,
      fetchMaxBytes:opt_fetchMaxBytes
    };

    let consumerCreationDelay = 0;
    if (delayConsumerCreation) {
      consumerCreationDelay = parseInt(delayConsumerCreation) || 60000;
      node.log(`Consumer creation delayed by ${consumerCreationDelay} millisecondes`);
    }

    let opt_connectTimeout = 0;
    if (connectTimeout) {
      opt_connectTimeout = parseInt(connectTimeout) || 10000;
    }

    const client = new kafka.KafkaClient({
      kafkaHost:kafkaHost,
      connectTimeout: opt_connectTimeout,
      reconnectOnIdle: true,
      autoConnect: true
    });


    const createConsumer = () => {
      const consumer = new kafka.Consumer(
        client,
        topics.split(',').map(topic => ({topic})),
        consumerOptions,
      );

      node.status({fill: 'green', shape: 'dot', text: `connected to ${kafkaHost}`});

      consumer.on('message', message => {
        if (debug) {
          console.log(message);
          node.log(message);
        }
        node.send({payload: message});
      });

      consumer.on('error', error => {
        if (debug) {
          console.error(error);
          node.error(error);
        }
        node.status({fill: 'red', shape: 'dot', text: `error: ${error}`});

      });
    };
    client.on('ready', () => {
      setTimeout(createConsumer, consumerCreationDelay);
    });

    client.on('error', (err) => {
      if (debug) {
        console.error(`err in client: ${err}\n`);
        node.error(`err in client: ${err}\n`);
      }
    });

  }

  RED.nodes.registerType('kafka-in', kafkaIn);

  function kafkaOut(config) {
    RED.nodes.createNode(this,config);
    const node = this;

    const { kafkaHost, topics, debug, connectTimeout } = config;
    const client = new kafka.KafkaClient({ kafkaHost, connectTimeout });

    client.on('ready', () => {
      const producer = new kafka.Producer(client);

      producer.on("ready",()=>{
        node.status({fill: 'green', shape: 'dot', text: `connected to ${kafkaHost}`});
      });
      producer.on("error", (error) => {
        node.error(`kafka out error: ${error}`);
        node.status({fill: 'red', shape: 'dot', text: `error: ${error}`});
      });

      node.on('input', message => {
        const payloads = topics.split(',').map(topic => ({ topic, messages: message.payload }));

        producer.send(payloads, (error, data) => {
          if (error) {
            node.error(error);
            node.status({fill: 'red', shape: 'dot', text: `error: ${error}`});
          }else{
            node.status({fill: 'green', shape: 'dot', text: `send success`});
          }
          if (debug) {
            node.log(`Data sent to Kafka: ${data}`);
          }
        });
      });
    });

    client.on('error', (err) => {
      if (debug) {
        console.error(`err in client: ${err}\n`);
        node.error(`err in client: ${err}\n`);
      }
      node.status({fill: 'red', shape: 'dot', text: `error: ${err}`});

    });


  }

  RED.nodes.registerType('kafka-out', kafkaOut);
};
