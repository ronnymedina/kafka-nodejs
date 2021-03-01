const { findSchemaBySubjectAndVersion, sendMessageToTopic, readMessageFromTopic } = require('./kafka')

const topic = 'info-users'
const version = 2
const subject = 'info-users-value'

const writeUserDataToKafka = async (payload) => {
  try {
    const encodePayloadId = await findSchemaBySubjectAndVersion({ version, subject })

    console.log(`Topic: ${topic}; subject: ${subject}; id: ${encodePayloadId}`)

    await sendMessageToTopic({ payload, topic, encodePayloadId })

  } catch (err) {
    console.error(err)
  }
}

const readMessages = () => {
  readMessageFromTopic(topic, (data) => {
    console.log(data, 'data desde kafka')
  })
}

module.exports.writeUserDataToKafka = writeUserDataToKafka
module.exports.readMessages = readMessages
