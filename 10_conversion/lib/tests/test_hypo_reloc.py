"""Step 40 readers for GOLD.SUM, b-run.sum (HYPOELLIPSE summary records) and PinatuboRelocs.rtf (PEPI)."""
import math

from lib.hypo_reloc import parse_hypoellipse, parse_pepi

GOLD = '9105181334252915N 940120E2016  30010  7288  3   8 2515  4112942 130  10P  74AF 3/    1292a       115.1.6 163 0'
BRUN = '19910601 017181515N 994120E2026  50211  8 92  2   432610  3623221  46  11  156AF 3/     5 1 0         .9.8 311 0  502'
BRUN_UNLOCATED = '19910501 310      N       W         15                                 15      K  /     5 1 0                        '


def test_pre_y2k_record(tmp_path):
    path = tmp_path / 'GOLD.SUM'
    path.write_text(GOLD + '\n91051909033393' + ' ' * 66 + '\n')
    origins, rejects = parse_hypoellipse(path, post_y2k=False)
    o = origins.iloc[0]
    assert o.origin_time == '1991-05-18T13:34:25.290000'
    assert math.isclose(o.latitude, 15 + 9.40 / 60, abs_tol=1e-6) and math.isclose(o.longitude, 120 + 20.16 / 60, abs_tol=1e-6)
    assert (o.depth_km, o.magnitude, o.processing_date, o.source_line) == (3.0, 1.0, '1992-12', 1)
    assert list(rejects.reason) == ['not_a_summary_record']


def test_post_y2k_record_and_unlocated(tmp_path):
    path = tmp_path / 'b-run.sum'
    path.write_text(BRUN + '\n' + BRUN_UNLOCATED + '\n')
    origins, rejects = parse_hypoellipse(path, post_y2k=True)
    o = origins.iloc[0]
    assert o.origin_time == '1991-06-01T00:17:18.150000'
    assert (o.depth_km, o.magnitude, o.processing_date) == (5.02, 1.1, '2001-05')
    assert list(rejects.reason) == ['no_location'] and list(rejects.source_line) == [2]


def test_pepi(tmp_path):
    path = tmp_path / 'PinatuboRelocs.rtf'
    path.write_text('{\\rtf1 pinatubo [43]==>cat final.hypos.off\\\n'
                    '910530 0737 59.18 15.1612N 120.3211W   4.79 0.00   5 0.007   91530035\\\n'
                    'pinatubo [29]==>cat final.hypos.summ\\\n'
                    '910603 2041  5.82 15.1427N 120.3365W   2.98 0.60   6 0.013  91603096\\\n}')
    origins, rejects = parse_pepi(path)
    assert rejects.empty and len(origins) == 2
    assert origins.longitude.tolist() == [120.3211, 120.3365]  # written W, east
    assert origins.reported_magnitude.tolist() == [0.0, 0.6]   # step 43 treats the zero as missing
    assert origins.note.iloc[0].startswith('pepi_event:91530035; set:off')
    assert origins.note.iloc[1].startswith('pepi_event:91603096; set:summ')
    assert (origins.nass.tolist(), origins.rms.tolist()) == ([5, 6], [0.007, 0.013])
