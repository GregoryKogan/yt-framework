"""
Stage Discovery
===============

Automatic discovery of stages from stages/ directory.
"""

import importlib
import sys
from pathlib import Path
from typing import List, Type
import logging

from yt_framework.core.stage import BaseStage


def discover_stages(
    pipeline_dir: Path,
    logger: logging.Logger,
) -> List[Type[BaseStage]]:
    """
    Automatically discover all stage classes from stages/ directory.
    
    Searches for stage.py files in stages/*/ subdirectories and imports
    all BaseStage subclasses found in them.
    
    Directory structure expected:
        pipeline_dir/
            stages/
                stage_name_1/
                    stage.py  # Contains Stage class inheriting from BaseStage
                stage_name_2/
                    stage.py
    
    Args:
        pipeline_dir: Path to pipeline directory
        logger: Logger instance
    
    Returns:
        List of discovered stage classes
    """
    stages_dir = pipeline_dir / "stages"
    
    if not stages_dir.exists():
        logger.warning(f"Stages directory not found: {stages_dir}")
        return []
    
    discovered_stages: List[Type[BaseStage]] = []
    
    # Iterate through each subdirectory in stages/
    for stage_dir in sorted(stages_dir.iterdir()):  # Sort for consistent order
        if not stage_dir.is_dir():
            continue
        
        stage_file = stage_dir / "stage.py"
        if not stage_file.exists():
            logger.debug(f"Skipping {stage_dir.name}: no stage.py file")
            continue
        
        # Import the stage module dynamically
        stage_name = stage_dir.name
        module_name = f"stages.{stage_name}.stage"
        
        try:
            # Add pipeline_dir to sys.path temporarily if needed
            if str(pipeline_dir) not in sys.path:
                sys.path.insert(0, str(pipeline_dir))
            
            # Import the module
            module = importlib.import_module(module_name)
            
            # Find all BaseStage subclasses in the module
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                
                # Check if it's a class, inherits from BaseStage, and isn't BaseStage itself
                if (isinstance(attr, type) and 
                    issubclass(attr, BaseStage) and 
                    attr is not BaseStage):
                    discovered_stages.append(attr)
                    logger.debug(f"Discovered stage: {stage_name} -> {attr.__name__}")
                    break  # Only take first BaseStage subclass per module
        
        except Exception as e:
            logger.warning(f"Failed to import stage from {stage_file}: {e}")
            continue
    
    if discovered_stages:
        stage_names = [sc.__name__ for sc in discovered_stages]
        logger.info(f"[Discovery] Found {len(discovered_stages)} stage{'s' if len(discovered_stages) != 1 else ''}: {', '.join(stage_names)}")
    return discovered_stages
