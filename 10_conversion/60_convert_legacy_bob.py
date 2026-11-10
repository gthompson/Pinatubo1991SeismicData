"""Convert legacy BOB ancillary records independently of waveform conversion."""
import argparse
from legacy_bob.runner import run

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',default=None,help='Shared conversion_config.json override')
    parser.add_argument('--products',nargs='+',choices=['cospec','counts','radio','rain','tilt'],default=None)
    args=parser.parse_args()
    for name,path in run(args.config,products=args.products).items():print(name,path)
if __name__=='__main__':main()
