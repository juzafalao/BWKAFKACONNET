package com.empresa.kafka.bw.service;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * ─────────────────────────────────────────────────────────────
 *  KafkaSendMessageService
 *  Chamado pela Java Activity do KafkaPublish.process no BW 5
 * ─────────────────────────────────────────────────────────────
 *
 *  No Designer, a Java Activity aponta para:
 *    Class Name:  com.empresa.kafka.bw.service.KafkaSendMessageService
 *    Method Name: sendFromBW
 *
 *  O método recebe String XML com os dados e retorna String XML com o resultado.
 *  Isso é necessário porque a Java Activity do BW 5 trabalha com tipos simples.
 */
public class KafkaSendMessageService {

    private static final Logger log =
        Logger.getLogger(KafkaSendMessageService.class.getName());

    private static final long SEND_TIMEOUT_SECONDS = 30L;

    /**
     * Método principal chamado pelo BW 5 via Java Activity.
     *
     * Recebe todos os parâmetros como String para compatibilidade
     * com a Java Activity do BW 5.13.
     *
     * @param bootstrapServers  "broker1:9092,broker2:9092"
     * @param authUsername      usuário SCRAM
     * @param authPassword      senha SCRAM
     * @param topic             tópico destino
     * @param key               chave da mensagem (pode ser null)
     * @param message           corpo da mensagem
     * @param headersXml        headers em XML simples (pode ser null)
     * @return resultado em XML: "<result><status/><topic/><partition/><offset/></result>"
     */
    public String sendFromBW(
            String bootstrapServers,
            String authUsername,
            String authPassword,
            String topic,
            String key,
            String message,
            String headersXml
    ) {
        log.info("[KafkaSendMessageService] Iniciando envio para topic=" + topic);

        KafkaProducer<String, String> producer = null;

        try {
            // 1. Valida inputs obrigatórios
            if (topic == null || topic.trim().isEmpty()) {
                return buildErrorResult("TOPIC_EMPTY", "O campo 'topic' é obrigatório.");
            }
            if (message == null || message.trim().isEmpty()) {
                return buildErrorResult("MESSAGE_EMPTY", "O campo 'message' é obrigatório.");
            }

            // 2. Monta e cria o Producer
            Properties props = buildProducerProperties(
                bootstrapServers, authUsername, authPassword);
            producer = new KafkaProducer<>(props);

            // 3. Monta os headers Kafka
            List<org.apache.kafka.common.header.Header> kafkaHeaders =
                parseHeaders(headersXml);

            // 4. Cria o record
            ProducerRecord<String, String> record = new ProducerRecord<>(
                topic,
                null,           // partition — Kafka decide
                key,
                message,
                kafkaHeaders
            );

            // 5. Envia e aguarda confirmação com timeout
            RecordMetadata metadata = producer
                .send(record)
                .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            log.info("[KafkaSendMessageService] Enviado com sucesso." +
                " topic=" + metadata.topic() +
                " partition=" + metadata.partition() +
                " offset=" + metadata.offset());

            return buildSuccessResult(
                metadata.topic(),
                metadata.partition(),
                metadata.offset(),
                metadata.timestamp()
            );

        } catch (Exception e) {
            log.severe("[KafkaSendMessageService] Erro ao enviar: " + e.getMessage());
            return buildErrorResult("SEND_FAILED", e.getMessage());

        } finally {
            // Fecha o producer após cada chamada
            // (em produção considere um pool, mas para POC está correto)
            if (producer != null) {
                try {
                    producer.flush();
                    producer.close();
                } catch (Exception ex) {
                    log.warning("[KafkaSendMessageService] Erro ao fechar producer: "
                        + ex.getMessage());
                }
            }
        }
    }

    // ─────────────────────────────────────────────────────────────
    //  Monta as Properties do Kafka com SASL/SCRAM-SHA-512
    // ─────────────────────────────────────────────────────────────
    private Properties buildProducerProperties(
            String bootstrapServers,
            String authUsername,
            String authPassword) {

        Properties props = new Properties();

        // Conexão
        props.put("bootstrap.servers",   bootstrapServers);
        props.put("request.timeout.ms",  "40000");
        props.put("retry.backoff.ms",    "100");
        props.put("reconnect.backoff.ms","50");
        props.put("metadata.max.age.ms", "300000");

        // Serializers
        props.put("key.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");

        // Producer
        props.put("acks",          "1");
        props.put("retries",       "3");
        props.put("batch.size",    "16384");
        props.put("linger.ms",     "5");
        props.put("buffer.memory", "33554432");

        // Segurança — SASL/SCRAM-SHA-512
        // (extraído do KafkaConnectionResource.kafkaconnectionResource do BW6)
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism",    "SCRAM-SHA-512");
        props.put("sasl.jaas.config",
            "org.apache.kafka.common.security.scram.ScramLoginModule required " +
            "username=\"" + authUsername + "\" " +
            "password=\"" + authPassword + "\";");

        return props;
    }

    // ─────────────────────────────────────────────────────────────
    //  Parse simples de headers vindos do processo BW como XML
    //  Formato esperado: <header><key>k</key><value>v</value></header>
    // ─────────────────────────────────────────────────────────────
    private List<org.apache.kafka.common.header.Header> parseHeaders(String headersXml) {
        List<org.apache.kafka.common.header.Header> headers = new ArrayList<>();

        if (headersXml == null || headersXml.trim().isEmpty()) {
            return headers;
        }

        try {
            // Parse manual simples — evita dependência de parser XML extra
            String[] parts = headersXml.split("<header>");
            for (int i = 1; i < parts.length; i++) {
                String part = parts[i];
                String key   = extractTag(part, "key");
                String value = extractTag(part, "value");
                if (key != null && !key.isEmpty()) {
                    headers.add(new RecordHeader(
                        key,
                        (value != null ? value : "").getBytes(StandardCharsets.UTF_8)
                    ));
                }
            }
        } catch (Exception e) {
            log.warning("[KafkaSendMessageService] Erro ao parsear headers: " + e.getMessage());
        }

        return headers;
    }

    private String extractTag(String xml, String tag) {
        String open  = "<" + tag + ">";
        String close = "</" + tag + ">";
        int start = xml.indexOf(open);
        int end   = xml.indexOf(close);
        if (start >= 0 && end > start) {
            return xml.substring(start + open.length(), end).trim();
        }
        return null;
    }

    // ─────────────────────────────────────────────────────────────
    //  Builders de resultado — retornam XML simples para o BW 5
    // ─────────────────────────────────────────────────────────────
    private String buildSuccessResult(
            String topic, int partition, long offset, long timestamp) {
        return "<result>" +
               "<status>SUCCESS</status>" +
               "<topic>" + topic + "</topic>" +
               "<partition>" + partition + "</partition>" +
               "<offset>" + offset + "</offset>" +
               "<timestamp>" + timestamp + "</timestamp>" +
               "<errorMessage></errorMessage>" +
               "</result>";
    }

    private String buildErrorResult(String code, String message) {
        // Escapa caracteres especiais XML
        String safeMsg = message != null
            ? message.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            : "";
        return "<result>" +
               "<status>ERROR</status>" +
               "<topic></topic>" +
               "<partition>-1</partition>" +
               "<offset>-1</offset>" +
               "<timestamp>-1</timestamp>" +
               "<errorMessage>[" + code + "] " + safeMsg + "</errorMessage>" +
               "</result>";
    }
}
