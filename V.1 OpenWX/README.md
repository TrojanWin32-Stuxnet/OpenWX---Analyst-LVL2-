# OpenWX---Analyst-LVL2-
### An open-source Level II Doppler radar analysis suite featuring real-time data streaming, custom dual-pol visualization, and integrated HRRR/GFS weather modeling for advanced storm tracking.

# OpenWX - Analyst LVL2

OpenWX Analyst LVL2 is a high-performance, open-source weather analysis platform designed to process and visualize raw NEXRAD Level II radar data. By combining direct data feeds with modern numerical weather prediction (NWP) models, it provides professional-grade insights for meteorologists, researchers, and storm chasers.

##  Key Features

* **Raw Level II Processing:** Real-time ingest of Base Reflectivity, Radial Velocity, and Spectrum Width.
* **Dual-Pol Analysis:** Specialized views for Correlation Coefficient (CC) and Differential Reflectivity (ZDR) to identify debris signatures and hydrometeor types.
* **Integrated Modeling:** Overlay HRRR (High-Resolution Rapid Refresh) and GFS model data directly onto radar views for predictive analysis.
* **Storm Cell Tracking:** Automated algorithms for Vertically Integrated Liquid (VIL) calculation and Mesocyclone detection.
* **GIS Layers:** High-resolution topography, power grid overlays, and road networks for impact assessment.

##  Quick Start

### Prerequisites
* Python 3.9+
* OpenGL 4.0+ (for hardware-accelerated rendering)
* [AWS CLI](https://aws.amazon.com/cli/) (for accessing NOAA NEXRAD S3 buckets)

### Installation
1. Clone the repository:
   ```bash
   git clone [https://github.com/TrojanWin32-Stuxnet/OpenWX-Analyst-LVL2.git](https://github.com/TrojanWin32-Stuxnet/OpenWX-Analyst-LVL2.git)
   cd OpenWX-Analyst-LVL2
