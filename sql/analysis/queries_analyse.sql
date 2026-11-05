-- Idée de requêtes d'analyse pour valider votre modèle gold.

-- Les requêtes "métier" ne sont pas implémenté pour ne pas 
-- donner trop d'indices sur le data modèle attendu. Mais vous
-- êtes libre d'essayer d'implémenter ces requêtes pour valider
-- votre conception.

-- 1. Chiffre d'affaires par mois et par devise

-- 2. Top 10 des compagnies aériennes par chiffre d'affaires

-- 3. Répartition des réservations par classe de cabine

-- 4. Volume de réservations par jour de la semaine

-- 5. Top 10 des aéroports de départ générant le plus de réservations
-- (airport_id sur une réservation = origin_airport_id du vol réservé)

-- 6. Évolution quotidienne du nombre de réservations sur septembre

-- 7. Chiffre d'affaires normalisé en EUR

-- 8. Durée moyenne de vol par compagnie

-- 9. Chiffre d'affaires par tranche d'âge de passager

-- Bonus SCD2 : quel avion était affecté à un vol à une date donnée ?
SELECT flight_id, aircraft_type, valid_from, valid_to
FROM dim_flight_history
WHERE flight_id = 'F0141'
  AND DATE '2025-09-05' >= valid_from AND DATE '2025-09-05' < valid_to;

-- Bonus SCD2 : réservations faites sur une version de vol qui a changé depuis
-- (avion, horaire ou date différents de la version courante).
SELECT fb.booking_id, fb.booking_date, h.aircraft_type AS avion_a_la_reservation,
       f.aircraft_type AS avion_actuel, h.flight_date AS date_prevue, f.flight_date AS date_actuelle
FROM fact_booking fb
JOIN dim_flight_history h USING (flight_sk)
JOIN dim_flight f ON f.flight_id = fb.flight_id
WHERE NOT h.is_current
ORDER BY fb.booking_date;
