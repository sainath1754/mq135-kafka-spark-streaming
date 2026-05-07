#define MQ135_PIN A0

// Load resistance on MQ135 module
#define RL_VALUE 10.0

// Clean air factor for MQ135
#define RO_CLEAN_AIR_FACTOR 3.6

float R0 = 10.0;

void setup() {

  Serial.begin(9600);

  Serial.println("======================================");
  Serial.println(" MQ135 CO2 Monitoring System Starting ");
  Serial.println("======================================");

  delay(3000);

  Serial.println("Calibrating sensor...");

  // Initial calibration
  float rs = getRS();

  R0 = rs / RO_CLEAN_AIR_FACTOR;

  Serial.print("Calibration complete. R0 = ");
  Serial.println(R0);

  Serial.println("Starting live monitoring...");
}

void loop() {

  float rs = getRS();

  float ratio = rs / R0;

  // CO2 estimation formula
  float ppm = 116.6020682 * pow(ratio, -2.769034857);

  // Clamp realistic range
  if (ppm < 350) {
    ppm = 350;
  }

  if (ppm > 5000) {
    ppm = 5000;
  }

  // Moving average simulation
  static float avg_ppm = ppm;

  avg_ppm = (avg_ppm * 0.8) + (ppm * 0.2);

  String status;

  if (ppm < 800) {

    status = "FRESH";

  } else if (ppm < 1200) {

    status = "NORMAL";

  } else if (ppm < 2000) {

    status = "WARNING";

  } else {

    status = "DANGER";
  }

  // Final serial payload
  Serial.print("RAW_CO2:");
  Serial.print(ppm);

  Serial.print("|AVG_CO2:");
  Serial.print(avg_ppm);

  Serial.print("|CO2_STATUS:");
  Serial.println(status);

  delay(1000);
}

// ─────────────────────────────────────────────

float getRS() {

  int adc = analogRead(MQ135_PIN);

  float voltage = adc * (5.0 / 1023.0);

  // Prevent divide-by-zero
  if (voltage < 0.1) {
    voltage = 0.1;
  }

  float rs = ((5.0 - voltage) / voltage) * RL_VALUE;

  return rs;
}