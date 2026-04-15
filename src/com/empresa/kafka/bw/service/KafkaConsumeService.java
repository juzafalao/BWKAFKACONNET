package com.empresa.kafka.bw.service;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.header.Header;

import java.time.Duration;
import java.util.*;
import java.util.logging.Logger;

/**
 * ─────────────────────────────────────────────────────────────
 *  KafkaConsumeService
 *  Chamado pela Java Activity do KafkaConsumer.process no BW 5
 * ─────────────────────────────────────────────────────────────
 *
 *  No Designer, a Java Activity aponta para:
 *    Class Name:  com.empresa.kafka.bw.service.KafkaConsumeService
 *    Method Name: consumeFromBW
 */
public class KafkaConsumeService {

    private static final Logger log =
        Logger.getLogger(KafkaConsumeService.class.getName());

    /**
     * Método principal chamado pelo BW 5 via Java Activity.
     *
     * @param bootstrapServers  "broker1:9092,broker2:9092"
     * @param authUsername      usuário SCRAM
     * @param authPassword      senha SCRAM
     * @param topic             tópico a consumir
     * @param groupId           consumer group id
     * @param maxMessages       máximo de mensagens (default 10)
     * @param pollTimeoutMs     timeout do poll em ms (default 5000)
     * @param autoCommit        commit automático (default true)
     * @return XML com as mensagens consumidas
     */
    public String consumeFromBW(
            String bootstrapServers,
            String authUsername,
            String authPassword,
            String topic,
            String groupId,
            String maxMessages,
            String pollTimeoutMs,
            String autoCommit
    ) {
        log.info("[KafkaConsumeService] Poll em topic=" + topic + " group=" + groupId);

        // Valores com defaults
        int  max     = parseIntOrDefault(maxMessages,   10);
        long timeout = parseLongOrDefault(pollTimeoutMs, 5000L);
        boolean commitAuto = !"false".equalsIgnoreCase(autoCommit);

        // Validações
        if (topic == null || topic.trim().isEmpty()) {
            return buildErrorResult("TOPIC_EMPTY", "O campo 'topic' é obrigatório.");
        }
        if (groupId == null || groupId.trim().isEmpty()) {
            return buildErrorResult("GROUPID_EMPTY", "O campo 'groupId' é obrigatório.");
        }

        Properties props = buildConsumerProperties(
            bootstrapServers, authUsername, authPassword, groupId, max, commitAuto);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

            consumer.subscribe(Collections.singletonList(topic));

            ConsumerRecords<String, String> records =
                consumer.poll(Duration.ofMillis(timeout));

            log.info("[KafkaConsumeService] Recebidas " + records.count() + " mensagens.");

            // Commit manual se autoCommit=false
            if (!commitAuto) {
                consumer.commitSync();
                log.info("[KafkaConsumeService] Offset commitado manualmente.");
            }

            return buildSuccessResult(records);

        } catch (Exception e) {
            log.severe("[KafkaConsumeService] Erro no consumo: " + e.getMessage());
            return buildErrorResult("CONSUME_FAILED", e.getMessage());
        }
    }

    // ─────────────────────────────────────────────────────────────
    //  Properties do Consumer com SASL/SCRAM-SHA-512
    // ─────────────────────────────────────────────────────────────
    private Properties buildConsumerProperties(
            String bootstrapServers,
            String authUsername,
            String authPassword,
            String groupId,
            int maxMessages,
            boolean autoCommit) {

        Properties props = new Properties();

        // Conexão
        props.put("bootstrap.servers",    bootstrapServers);
        props.put("request.timeout.ms",   "40000");
        props.put("retry.backoff.ms",     "100");
        props.put("metadata.max.age.ms",  "300000");

        // Deserializers
        props.put("key.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer");

        // Consumer
        props.put("group.id",                groupId);
        props.put("enable.auto.commit",      String.valueOf(autoCommit));
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset",       "earliest");
        props.put("max.poll.records",        String.valueOf(maxMessages));
        props.put("session.timeout.ms",      "30000");
        props.put("heartbeat.interval.ms",   "3000");

        // Segurança SASL/SCRAM-SHA-512
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism",    "SCRAM-SHA-512");
        props.put("sasl.jaas.config",
            "org.apache.kafka.common.security.scram.ScramLoginModule required " +
            "username=\"" + authUsername + "\" " +
            "password=\"" + authPassword + "\";");

        return props;
    }

    // ─────────────────────────────────────────────────────────────
    //  Builders de resultado XML
    // ─────────────────────────────────────────────────────────────
    private String buildSuccessResult(ConsumerRecords<String, String> records) {
        StringBuilder sb = new StringBuilder();
        sb.append("<r>");
        sb.append("<status>SUCCESS</status>");
        sb.append("<totalMessages>").append(records.count()).append("</totalMessages>");
        sb.append("<errorMessage></errorMessage>");
        sb.append("<messages>");

        for (ConsumerRecord<String, String> record : records) {
            sb.append("<message>");
            sb.append("<topic>").append(escape(record.topic())).append("</topic>");
            sb.append("<partition>").append(record.partition()).append("</partition>");
            sb.append("<offset>").append(record.offset()).append("</offset>");
            sb.append("<key>").append(escape(record.key())).append("</key>");
            sb.append("<value>").append(escape(record.value())).append("</value>");
            sb.append("<timestamp>").append(record.timestamp()).append("</timestamp>");

            // Headers
            sb.append("<headers>");
            if (record.headers() != null) {
                for (Header h : record.headers()) {
                    sb.append("<header>");
                    sb.append("<key>").append(escape(h.key())).append("</key>");
                    sb.append("<value>")
                      .append(escape(new String(h.value())))
                      .append("</value>");
                    sb.append("</header>");
                }
            }
            sb.append("</headers>");
            sb.append("</message>");
        }

        sb.append("</messages>");
        sb.append("</r>");
        return sb.toString();
    }

    private String buildErrorResult(String code, String message) {
        String safeMsg = message != null
            ? message.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            : "";
        return "<r>" +
               "<status>ERROR</status>" +
               "<totalMessages>0</totalMessages>" +
               "<errorMessage>[" + code + "] " + safeMsg + "</errorMessage>" +
               "<messages/>" +
               "</r>";
    }

    private String escape(String value) {
        if (value == null) return "";
        return value
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;");
    }

    private int parseIntOrDefault(String value, int defaultVal) {
        try { return (value != null && !value.trim().isEmpty())
            ? Integer.parseInt(value.trim()) : defaultVal; }
        catch (NumberFormatException e) { return defaultVal; }
    }

    private long parseLongOrDefault(String value, long defaultVal) {
        try { return (value != null && !value.trim().isEmpty())
            ? Long.parseLong(value.trim()) : defaultVal; }
        catch (NumberFormatException e) { return defaultVal; }
    }
}
