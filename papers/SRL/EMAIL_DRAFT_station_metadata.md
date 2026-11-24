**To:** Andy Lockhart, Tom Murray, John Power, Randy White, Dave Harlow
**Cc (suggested):** Pat McChesney (J502/VCO electronics); Gemme Ambubuyog (PHIVOLCS; GRN calibration pulses); Chris Newhall
**Subject:** Pinatubo 1991 seismic network: sensors and gain settings for the station metadata

Dear Andy, Tom, John, Randy and Dave,

I am building StationXML station metadata for the recovered 1991 Pinatubo seismic data. The waveforms are now on UTC, so the remaining gaps are the instruments and their gain settings. Anything you remember, or any notebook, form or calibration record you still have, would help, even if it only settles one station.

The main source so far is Andy's field synopsis (Lockhart et al., 1996, Appendix 1). The period I most need to get right is up to 16 June, because follow-up papers will look at the escalation sequence.

**What I have now, to 16 June**

Dates are local (PHT), and attenuations are VCO attenuator settings in dB.

| Station | Sensor | Attenuation and changes |
|---|---|---|
| LIL | L-4 | 30 dB, 25–30 April (temporary; no waveforms survive) |
| PIE | L-4 | 18 dB from 30 April; destroyed 15 June |
| UBO | L-4 | 36 dB from 1 May (data start the evening of 30 April); signal lost 22:00 on 11 June |
| CAB | L-4 | 30 dB from 1 May; 24 dB from 12:45 on 5 May; offline late May – early June (battery); back on 11 June |
| PPO | L-4 Z | 36 dB, 3–12 May |
| PPO | L-22D, three-component, from 12 May | Z 24 dB, N and E 30 dB; Z to 30 dB on 30 May; lost with Bunker Hill on 11 June |
| BUR | L-4 | 24 dB from 5 May; lost with Bunker Hill on 11 June |
| BUG | L-4 | 18 dB from 11 May; lightning 24 May; reinstalled at 18 dB on 28 May; lost 15 June |
| GRN | L-4 | attenuation never written down; 24 dB inferred after the eruption from VCO calibration pulses; destroyed 15 June |

For the rest of the chain I have had to use values documented for the equipment types, not for the units deployed:
- **VCO amplifier:** 90 dB gain before the attenuator, which is the USGS J502 figure (McChesney, 1999), since Tom's boards were built on the J502 circuit.
- **VCO deviation:** 115 Hz per 4.05 V.
- **Discriminators:** J101 and J110, assumed to invert that deviation.
- **Digitizer:** a DT2821 on WILLIE, at ±2.5 V full scale (input gain 4, from the SUDS headers), which is 819.2 counts per volt.

One check is encouraging. Andy wrote that gains were set for 100 mV peak-to-peak of noise at the VCO input. With these values that should be about 82 counts. The recorded pre-trigger noise in the first days of each station is 90–160 counts, about the same at every station regardless of attenuation. The exception is PPO after the L-22D went in, at only 37–59 counts.

**Questions**

1. **Sensors.** Helicorder notes compiled recently describe an L-1 (1 Hz vertical) at PIE and UBO and a three-component L-22 at CAB, where Andy's synopsis has L-4s.
   - Which is right?
   - If the helicorder notes are right, was there a later swap, or were those different recording paths?
   - Do you recall coil resistances, damping shunts or serial numbers, for the L-4s and for the PPO L-22D in particular?
2. **VCO boards.**
   - What was the gain of Tom's boards at 0 dB attenuation, and what attenuator steps were available?
   - Was the deviation 115 Hz per 4.05 V, like the J502, or something else (for example 125 Hz per 3 V)?
   - Were the boards' calibration signals recorded anywhere, with a known input amplitude?
3. **Discriminators and digitizer.**
   - What were the output scale and filter settings of the J101 and J110 discriminators feeding WILLIE?
   - Was the DT2821 run at ±2.5 V full scale (gain 4) on the seismic channels?
   - Was there an anti-alias filter ahead of it?
   - Did the same discriminator outputs feed the Tandy RSAM and the helicorders, so that RSAM and the waveforms share the same gain chain?
4. **Changes not in the synopsis.** Were any gains or sensors changed between 1 and 16 June that are not in the synopsis? In particular:
   - **CAB:** settings when it came back on 11 June. Its helicorder records begin about 12 June.
   - **PPO:** whether the low noise after 12 May means a different setting or sensor.
   - **GRN:** whether 24 dB is right.
   - **UBO and PIE:** whether anything changed in their final days.
5. **After the eruption** (late June onward). The waveforms show BUG, CAB (Z, and an N component from 3 July), CRW, DON, FNG, PI2 (normal gain, and a low-gain PI2L in the station lists) and QAD.
   - Do you know the sensors, attenuations and installation dates for these?
   - An 8 July frequency map gives 36 dB for PI2Z and "?" for PI2L.
6. **Records.** Do any field notebooks, station forms, calibration logs or the attenuator-setting notes survive, at CVO, at PHIVOLCS, or in someone's files? Gemme's analysis of the GRN calibration pulses would be especially useful if it still exists.

Corrections to anything in the table are just as welcome as new information. Unless you tell me otherwise, I will acknowledge your help in the paper.

With thanks,
Glenn

---
*Notes for Glenn, not part of the email:*
- **Recipients:** add email addresses.
- **Second batch:** consider sending to McChesney, Ambubuyog and Newhall separately.
- **Wendy:** she supplied the selected pre-eruption station table; step 70 doesn't record her surname. If she was involved, add her too.
- **Sources for the figures quoted:**
  - noise figures: `response_noise_check.csv`;
  - assumed values: `10_conversion/nominal_electronics.json`;
  - per-epoch table: supplement, "Station metadata and nominal responses".
